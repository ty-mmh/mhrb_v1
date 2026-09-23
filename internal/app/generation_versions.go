package app

import "mahoroba.local/mahoroba/internal/domain"

func pinGenerationVersions(value *domain.PrepareGeneration) error {
	versions, err := domain.GenerationVersionsForPurpose(value.Purpose)
	if err != nil {
		return err
	}
	value.PromptTemplateVersion = versions.PromptTemplateVersion
	value.ContextPolicyVersion = versions.ContextPolicyVersion
	value.MemoryRenderingVersion = versions.MemoryRenderingVersion
	return nil
}

func validatePreparedGenerationVersions(value domain.PreparedGeneration) error {
	return domain.ValidateGenerationVersions(value.Purpose, domain.GenerationVersionContract{
		PromptTemplateVersion:  value.PromptTemplateVersion,
		ContextPolicyVersion:   value.ContextPolicyVersion,
		MemoryRenderingVersion: value.MemoryRenderingVersion,
	})
}
