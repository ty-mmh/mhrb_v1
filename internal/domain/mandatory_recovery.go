package domain

import (
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/integrity"
)

const CancellationEnvelopeSemanticDomainV1 = "mahoroba:cancellation-envelope-semantic:v1"

// MandatoryRecoveryKind is the closed set of event-scoped obligations that
// offline recovery may terminalize without dispatching a provider request.
type MandatoryRecoveryKind string

const (
	MandatoryRecoveryDialogue         MandatoryRecoveryKind = "dialogue"
	MandatoryRecoveryMemoryExtraction MandatoryRecoveryKind = "memory_extraction"
)

func (kind MandatoryRecoveryKind) Validate() error {
	switch kind {
	case MandatoryRecoveryDialogue, MandatoryRecoveryMemoryExtraction:
		return nil
	default:
		return fmt.Errorf("domain: unsupported mandatory recovery kind %q", kind)
	}
}

// MandatoryRecoveryCursor is an opaque, stable event-order cursor. Event seq
// is resident-scoped, so the resident identity is part of the ordering key.
type MandatoryRecoveryCursor struct {
	ResidentID canonical.ID
	EventSeq   canonical.Seq
}

// MandatoryRecoveryWork is a provider-free cancellation candidate. A nil
// RunID denotes the pre-dispatch synthetic attempt-zero transition.
type MandatoryRecoveryWork struct {
	Kind             MandatoryRecoveryKind
	SourceEvent      Event
	IdempotencyKey   string
	PolicyRevisionID canonical.ID
	RunID            *canonical.ID
	AttemptNo        int64
	State            WorkState
	CancellationCode string
}

// CancellationEnvelopeResolution returns either a complete immutable
// generation envelope or the exact typed integrity candidate that explains
// why a run must not be invented. Exactly one field is populated.
type CancellationEnvelopeResolution struct {
	Generation PrepareGeneration
	Unresolved *integrity.CandidateInput
}

var ErrRecoveryAttemptOverflow = errors.New("domain: recovery attempt overflow")

// RecoveryAttemptOverflowError preserves the run identity needed by Admin's
// repair_static_design action while remaining matchable as a stable sentinel.
type RecoveryAttemptOverflowError struct {
	RunID canonical.ID
}

func (failure *RecoveryAttemptOverflowError) Error() string {
	if failure == nil {
		return ErrRecoveryAttemptOverflow.Error()
	}
	return fmt.Sprintf("%s: generation run %s is at math.MaxInt64", ErrRecoveryAttemptOverflow, failure.RunID)
}

func (*RecoveryAttemptOverflowError) Unwrap() error { return ErrRecoveryAttemptOverflow }

// CancellationEnvelopeSemanticDigest binds the restore-invariant meaning of
// a provider-free cancellation. Generated run/outcome IDs and the later
// cancellation commit time are intentionally absent; every field that can
// affect provider selection or replay meaning is included in exact JCS.
func CancellationEnvelopeSemanticDigest(
	envelope PrepareGeneration,
	reason string,
) (canonical.Digest, canonical.CanonicalJSON, error) {
	if err := envelope.ResidentID.Validate(); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	if err := envelope.Purpose.Validate(); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	purpose := envelope.Purpose
	if purpose != GenerationPurposeDialogue && purpose != GenerationPurposeMemoryExtraction {
		return canonical.Digest{}, canonical.CanonicalJSON{}, fmt.Errorf(
			"domain: purpose %q is not a mandatory cancellation purpose", purpose,
		)
	}
	if err := envelope.PipelineVersionID.Validate(); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	for _, revisionID := range []canonical.ID{
		envelope.PrinciplesRevisionID, envelope.PersonaRevisionID, envelope.MemoryPolicyRevisionID,
	} {
		if err := revisionID.Validate(); err != nil {
			return canonical.Digest{}, canonical.CanonicalJSON{}, err
		}
	}
	if purpose == GenerationPurposeDialogue {
		if envelope.SessionPolicyID == nil {
			return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: dialogue cancellation requires session policy")
		}
		if err := envelope.SessionPolicyID.Validate(); err != nil {
			return canonical.Digest{}, canonical.CanonicalJSON{}, err
		}
	} else if envelope.SessionPolicyID != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: non-dialogue cancellation cannot contain session policy")
	}
	if err := envelope.AsOfTZ.Validate(); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	versionContract := GenerationVersionContract{
		PromptTemplateVersion:  envelope.PromptTemplateVersion,
		ContextPolicyVersion:   envelope.ContextPolicyVersion,
		MemoryRenderingVersion: envelope.MemoryRenderingVersion,
	}
	if purpose == GenerationPurposeDialogue {
		if err := ValidateDialogueCancellationVersionContract(versionContract); err != nil {
			return canonical.Digest{}, canonical.CanonicalJSON{}, err
		}
	} else if err := ValidateGenerationVersions(envelope.Purpose, versionContract); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	if envelope.IdempotencyKey == "" || envelope.Provider != "mahoroba-internal" || envelope.Model != "not-dispatched" {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: incomplete cancellation semantic envelope")
	}
	code, err := generation.ParseOutcomeErrorCode(reason)
	if err != nil || code.Retryable() {
		return canonical.Digest{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: invalid cancellation semantic reason %q", reason)
	}
	allowedReason := code.Class() == generation.ErrorSourceContentErased ||
		code.Class() == generation.ErrorResidentInactive
	if purpose == GenerationPurposeDialogue {
		allowedReason = allowedReason || code.Class() == generation.ErrorResidentUnselected
	}
	if !allowedReason {
		return canonical.Digest{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: cancellation semantic reason %q is not valid for %s", reason, envelope.Purpose)
	}
	if envelope.GeneratorParams.IsZero() || envelope.DroppedInputSummary.IsZero() {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: cancellation semantic JSON dependencies are required")
	}
	params, _, err := ParseGeneratorParams(envelope.GeneratorParams.Bytes())
	if err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	if err := ValidateNewGeneratorParamsForPurpose(envelope.Purpose, params); err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	if params.Streaming || params.MaxOutputBytes != 1 {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New(
			"domain: cancellation generator params must be non-streaming with max_output_bytes 1",
		)
	}
	var expectedDropped canonical.CanonicalJSON
	if purpose == GenerationPurposeDialogue {
		expectedDropped, err = canonical.MarshalCanonical(struct {
			Backfill canonical.Count `json:"backfill"`
			Live     canonical.Count `json:"live_context"`
			Reason   string          `json:"reason"`
		}{Reason: reason})
	} else {
		expectedDropped, err = canonical.MarshalCanonical(struct {
			Reason string `json:"reason"`
		}{Reason: reason})
	}
	if err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	if envelope.DroppedInputSummary.String() != expectedDropped.String() {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: cancellation dropped-input summary mismatch")
	}
	if envelope.RecallRunID != nil || len(envelope.Inputs) != 0 {
		return canonical.Digest{}, canonical.CanonicalJSON{}, errors.New("domain: cancellation envelope cannot contain recall or inputs")
	}
	type sampling struct {
		MaxTokens   *int64 `json:"max_tokens"`
		Seed        *int64 `json:"seed"`
		Temperature *int64 `json:"temperature"`
		TopP        *int64 `json:"top_p"`
	}
	projection, err := canonical.MarshalCanonical(struct {
		AsOf                   canonical.Instant       `json:"as_of"`
		AsOfTZ                 canonical.Timezone      `json:"as_of_tz"`
		BudgetExceeded         bool                    `json:"budget_exceeded"`
		ContextPolicyVersion   string                  `json:"context_policy_version"`
		DroppedInputSummary    canonical.CanonicalJSON `json:"dropped_input_summary"`
		GeneratorParams        canonical.CanonicalJSON `json:"generator_params"`
		IdempotencyKey         string                  `json:"idempotency_key"`
		MemoryPolicyRevisionID canonical.ID            `json:"memory_policy_revision_id"`
		MemoryRenderingVersion string                  `json:"memory_rendering_version"`
		Model                  string                  `json:"model"`
		ModelVersion           *string                 `json:"model_version"`
		PersonaRevisionID      canonical.ID            `json:"persona_revision_id"`
		PipelineVersionID      canonical.ID            `json:"pipeline_version_id"`
		PrinciplesRevisionID   canonical.ID            `json:"principles_revision_id"`
		PromptTemplateVersion  string                  `json:"prompt_template_version"`
		Provider               string                  `json:"provider"`
		Purpose                GenerationPurpose       `json:"purpose"`
		Reason                 string                  `json:"reason"`
		ResidentID             canonical.ID            `json:"resident_id"`
		Sampling               sampling                `json:"sampling"`
		SessionPolicyID        *canonical.ID           `json:"sessionization_policy_version_id"`
	}{
		AsOf: envelope.AsOf, AsOfTZ: envelope.AsOfTZ, BudgetExceeded: envelope.BudgetExceeded,
		ContextPolicyVersion: envelope.ContextPolicyVersion,
		DroppedInputSummary:  envelope.DroppedInputSummary, GeneratorParams: envelope.GeneratorParams,
		IdempotencyKey: envelope.IdempotencyKey, MemoryPolicyRevisionID: envelope.MemoryPolicyRevisionID,
		MemoryRenderingVersion: envelope.MemoryRenderingVersion, Model: envelope.Model,
		PersonaRevisionID: envelope.PersonaRevisionID, PipelineVersionID: envelope.PipelineVersionID,
		PrinciplesRevisionID:  envelope.PrinciplesRevisionID,
		PromptTemplateVersion: envelope.PromptTemplateVersion, Provider: envelope.Provider,
		Purpose: purpose, Reason: reason, ResidentID: envelope.ResidentID,
		// Provider-free cancellation deliberately fixes unsupported provider
		// controls to null. They are included in the semantic projection so a
		// future contract cannot silently inherit restore-time configuration.
		ModelVersion: nil, Sampling: sampling{}, SessionPolicyID: envelope.SessionPolicyID,
	})
	if err != nil {
		return canonical.Digest{}, canonical.CanonicalJSON{}, err
	}
	material := make([]byte, 0, len(CancellationEnvelopeSemanticDomainV1)+1+len(projection.Bytes()))
	material = append(material, CancellationEnvelopeSemanticDomainV1...)
	material = append(material, 0)
	material = append(material, projection.Bytes()...)
	return canonical.HashBlob(material), projection, nil
}
