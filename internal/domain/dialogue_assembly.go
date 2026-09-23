package domain

import (
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

var ErrDialogueAssemblyTargetChanged = errors.New("domain: dialogue Assembly Target changed")

// DialogueAssemblyRequest supplies only identities, limits, and the immutable
// target needed by the read-side Context Assembler. Context selection and
// version choice are deliberately not caller-provided.
type DialogueAssemblyRequest struct {
	SourceEventID     canonical.ID
	ResidentID        canonical.ID
	Target            AssemblyTarget
	RunID             canonical.ID
	RunningOutcomeID  canonical.ID
	RecallRunID       canonical.ID
	InputIDs          []canonical.ID
	InputContentIDs   []canonical.ID
	InputContentSalts []canonical.ContentSalt
	RecallUsageIDs    []canonical.ID
	Provider          string
	Model             string
	GeneratorParams   canonical.CanonicalJSON
	MaxInputBytes     int64
	LiveEventLimit    int64
}

func (request DialogueAssemblyRequest) Validate() error {
	if err := request.Target.Validate(); err != nil {
		return err
	}
	if request.Provider == "" || request.Model == "" || request.GeneratorParams.IsZero() ||
		request.MaxInputBytes < 1 || request.LiveEventLimit != DialogueLiveEventLimit {
		return errors.New("domain: incomplete dialogue Assembly request")
	}
	params, _, err := ParseGeneratorParams(request.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeDialogue, params); err != nil {
		return err
	}
	if len(request.InputIDs) != MaxDialogueInputs || len(request.InputContentIDs) != MaxDialogueInputs ||
		len(request.InputContentSalts) != MaxDialogueInputs || len(request.RecallUsageIDs) != MaxRecallUsages {
		return errors.New("domain: dialogue Assembly requires complete fixed ID and salt pools")
	}
	seen := make(map[canonical.ID]string, 5+2*MaxDialogueInputs+MaxRecallUsages)
	for label, id := range map[string]canonical.ID{
		"source event": request.SourceEventID, "resident": request.ResidentID,
		"run": request.RunID, "running outcome": request.RunningOutcomeID, "Recall run": request.RecallRunID,
	} {
		if err := addDialogueAssemblyID(seen, id, label); err != nil {
			return err
		}
	}
	for index, id := range request.InputIDs {
		if err := addDialogueAssemblyID(seen, id, fmt.Sprintf("input[%d]", index)); err != nil {
			return err
		}
	}
	for index, id := range request.InputContentIDs {
		if err := addDialogueAssemblyID(seen, id, fmt.Sprintf("input content[%d]", index)); err != nil {
			return err
		}
	}
	for index, id := range request.RecallUsageIDs {
		if err := addDialogueAssemblyID(seen, id, fmt.Sprintf("Recall usage[%d]", index)); err != nil {
			return err
		}
	}
	return nil
}

func addDialogueAssemblyID(seen map[canonical.ID]string, id canonical.ID, label string) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("domain: invalid %s ID: %w", label, err)
	}
	if previous, duplicate := seen[id]; duplicate {
		return fmt.Errorf("domain: dialogue Assembly ID for %s duplicates %s", label, previous)
	}
	seen[id] = label
	return nil
}

type DialogueAssemblyResult struct {
	Prepare  PrepareDialogue
	Contents []Content
}
