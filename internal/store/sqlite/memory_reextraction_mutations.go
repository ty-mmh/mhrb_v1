package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

func (u *canonicalUoW) PrepareMemoryReextraction(
	ctx context.Context,
	value domain.PrepareMemoryReextraction,
) (domain.MemoryReextractionResult, error) {
	envelope := value.Generation
	if err := u.requireResidentScope(envelope.ResidentID); err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	request, err := domain.ParseMemoryExtractionObligation(envelope.IdempotencyKey)
	if err != nil || request.Mode != domain.MemoryExtractionReextract || request.RequestID == nil ||
		request.SourceEventID != value.SourceEventID || *request.RequestID != value.RequestID {
		return domain.MemoryReextractionResult{}, errors.New("sqlite: invalid memory re-extraction key")
	}

	var existingRaw string
	err = u.tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, envelope.ResidentID.String(), envelope.IdempotencyKey).
		Scan(&existingRaw)
	if err == nil {
		runID, parseErr := canonical.ParseID(existingRaw)
		if parseErr != nil {
			return domain.MemoryReextractionResult{}, parseErr
		}
		attempt, state, parseErr := u.latestOutcome(ctx, runID, envelope.ResidentID)
		if parseErr != nil {
			return domain.MemoryReextractionResult{}, parseErr
		}
		return domain.MemoryReextractionResult{
			RunID: runID, AttemptNo: attempt, State: generationStateToWorkState(state), Changed: false,
		}, canonical.ErrNoMutation
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryReextractionResult{}, fmt.Errorf("sqlite: inspect memory re-extraction request: %w", err)
	}
	if err := u.validateMemoryReextractionEnvelope(ctx, value); err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	if err := u.prepareGeneration(ctx, envelope, true); err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	return domain.MemoryReextractionResult{
		RunID: envelope.RunID, AttemptNo: 1, State: domain.WorkRunning, Changed: true,
	}, nil
}

func (u *canonicalUoW) validateMemoryReextractionEnvelope(
	ctx context.Context,
	value domain.PrepareMemoryReextraction,
) error {
	envelope := value.Generation
	if envelope.Purpose.Effective() != domain.GenerationPurposeMemoryExtraction ||
		envelope.SessionPolicyID != nil || envelope.RecallRunID != nil || len(envelope.Inputs) != 4 {
		return errors.New("sqlite: invalid memory re-extraction envelope shape")
	}
	if err := u.requireActiveResident(ctx, envelope.ResidentID); err != nil {
		return err
	}
	if err := u.requireActiveRevision(ctx, envelope.ResidentID, envelope.PrinciplesRevisionID, "principles"); err != nil {
		return err
	}
	if err := u.requireActiveRevision(ctx, envelope.ResidentID, envelope.PersonaRevisionID, "persona"); err != nil {
		return err
	}
	if err := u.requireActiveRevision(ctx, envelope.ResidentID, envelope.MemoryPolicyRevisionID, "memory_policy"); err != nil {
		return err
	}
	if err := u.requireExactMemoryPipeline(ctx, envelope.PipelineVersionID,
		"memory_extraction", domain.MemoryExtractionPipelineVersion); err != nil {
		return err
	}
	params, _, err := domain.ParseGeneratorParams(envelope.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	schema, err := memory.ExtractionJSONSchema()
	if err != nil {
		return err
	}
	if params.Streaming || params.SchemaVersion != memory.ExtractionOutputSchemaVersionV1 ||
		params.SchemaHash == nil || *params.SchemaHash != canonical.HashBlob(schema.Bytes()) {
		return errors.New("sqlite: memory re-extraction structured-output contract mismatch")
	}
	if params.StructuredOutputMode != generation.StructuredOutputPrompt &&
		params.StructuredOutputMode != generation.StructuredOutputJSONSchema {
		return errors.New("sqlite: invalid memory re-extraction structured-output mode")
	}

	var eventType, erasureState, recordedTZ string
	var sourceContent []byte
	var recordedAt int64
	if err := u.tx.QueryRowContext(ctx, `SELECT event.event_type, content.erasure_state,
		event.recorded_at, event.recorded_tz, blob.content
		FROM events event
		JOIN content_objects content ON content.content_id = event.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE event.event_id = ? AND event.resident_id = ?`,
		value.SourceEventID.String(), envelope.ResidentID.String()).Scan(
		&eventType, &erasureState, &recordedAt, &recordedTZ, &sourceContent,
	); err != nil {
		return fmt.Errorf("sqlite: load memory re-extraction source: %w", err)
	}
	if eventType != string(memory.EventUserMessage) || erasureState != "present" || sourceContent == nil {
		return errors.New("sqlite: memory re-extraction source is unavailable")
	}
	if envelope.AsOf != canonical.Instant(recordedAt) || envelope.AsOfTZ.String() != recordedTZ {
		return errors.New("sqlite: memory re-extraction as-of differs from source event")
	}

	expectedSources := []struct {
		role, sourceType, inclusion string
		id                          canonical.ID
		content                     []byte
	}{
		{string(generation.RoleSystem), "resident_revision", "resident_definition", envelope.PrinciplesRevisionID, nil},
		{string(generation.RoleSystem), "resident_revision", "resident_definition", envelope.PersonaRevisionID, nil},
		{string(generation.RoleSystem), "resident_revision", "resident_definition", envelope.MemoryPolicyRevisionID, nil},
		{string(generation.RoleUser), "event", "current_input", value.SourceEventID, sourceContent},
	}
	for index := range expectedSources[:3] {
		content, err := u.residentRevisionContent(ctx, envelope.ResidentID, expectedSources[index].id)
		if err != nil {
			return err
		}
		expectedSources[index].content = content
	}
	policy, _, err := memory.ParsePolicy(expectedSources[2].content)
	if err != nil {
		return err
	}
	if err := policy.RequireEnabled(); err != nil {
		return err
	}
	for index, want := range expectedSources {
		input := envelope.Inputs[index]
		if input.Ordinal != int64(index) || input.Role != want.role || input.SourceType != want.sourceType ||
			input.InclusionMode != want.inclusion || input.SourceID == nil || *input.SourceID != want.id ||
			!bytes.Equal(input.Content.Bytes, want.content) {
			return fmt.Errorf("sqlite: memory re-extraction input %d does not match its Canonical source", index)
		}
	}
	return nil
}

func (u *canonicalUoW) residentRevisionContent(
	ctx context.Context,
	residentID, revisionID canonical.ID,
) ([]byte, error) {
	var content []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revisions revision
		JOIN content_objects object ON object.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE revision.revision_id = ? AND revision.resident_id = ?`,
		revisionID.String(), residentID.String()).Scan(&content); err != nil {
		return nil, fmt.Errorf("sqlite: load memory re-extraction revision content: %w", err)
	}
	if content == nil {
		return nil, errors.New("sqlite: memory re-extraction revision content is erased")
	}
	return content, nil
}

func (u *canonicalUoW) requireExactMemoryPipeline(
	ctx context.Context,
	pipelineID canonical.ID,
	wantKind, wantVersion string,
) error {
	var kind, version, definition string
	if err := u.tx.QueryRowContext(ctx, `SELECT pipeline_kind, version_key, definition
		FROM pipeline_versions WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(
		&kind, &version, &definition,
	); err != nil {
		return fmt.Errorf("sqlite: load memory pipeline: %w", err)
	}
	wantDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: wantVersion})
	if err != nil {
		return err
	}
	if kind != wantKind || version != wantVersion || definition != wantDefinition.String() {
		return fmt.Errorf("sqlite: pipeline %s does not match %s/%s", pipelineID, wantKind, wantVersion)
	}
	return nil
}

func generationStateToWorkState(state string) domain.WorkState {
	switch state {
	case "running":
		return domain.WorkRunning
	case "succeeded":
		return domain.WorkSucceeded
	case "failed", "cancelled":
		return domain.WorkTerminalFailed
	default:
		return domain.WorkTerminalFailed
	}
}
