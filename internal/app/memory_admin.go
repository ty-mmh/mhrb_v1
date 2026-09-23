package app

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type ActivateMemoryPolicyV4Options struct {
	ResidentID                    canonical.ID
	ExpectedFrom                  memory.PolicyVersion
	AcknowledgeRecallEnable       bool
	AcknowledgeSelfTalkExtraction bool
}

// memoryPolicyProjectionRebuilder is deliberately narrower than the normal
// asynchronous commit notifier. A V4 transition changes the policy dependency
// and must not reopen an active resident until every required Projection has
// been rebuilt against the post-activation Canonical head.
type memoryPolicyProjectionRebuilder interface {
	RebuildAll(context.Context, canonical.ID) error
}

// ActivateMemoryPolicyV0 is the historical V2 compatibility endpoint. It may
// only converge a lost-response retry when V2 is already active.
func (a *Application) ActivateMemoryPolicyV0(
	ctx context.Context,
	residentID canonical.ID,
) (domain.MemoryPolicyActivationResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if resident.Status != "active" {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("app: memory policy activation requires active resident")
	}
	current, err := parseResidentMemoryPolicy(resident)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if current.Version != memory.PolicyVersionV2 {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf(
			"app: legacy memory-policy-v2 activation only retries an active v2 policy; use ActivateMemoryPolicyV4",
		)
	}
	return a.activateMemoryPolicy(ctx, resident, memory.DefaultPolicyV2(), memoryPolicyActivationTransition{
		expectedFrom: memory.PolicyVersionV2,
	})
}

// ActivateAutonomyMemoryPolicyV0 is the historical V3 compatibility endpoint.
// It may only converge a lost-response retry when V3 is already active.
func (a *Application) ActivateAutonomyMemoryPolicyV0(
	ctx context.Context,
	residentID canonical.ID,
) (domain.MemoryPolicyActivationResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if resident.Status != "active" {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("app: memory policy activation requires active resident")
	}
	current, err := parseResidentMemoryPolicy(resident)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if current.Version != memory.PolicyVersionV3 {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf(
			"app: legacy memory-policy-v3 activation only retries an active v3 policy; use ActivateMemoryPolicyV4",
		)
	}
	return a.activateMemoryPolicy(ctx, resident, memory.DefaultPolicyV3(), memoryPolicyActivationTransition{
		expectedFrom: memory.PolicyVersionV3,
	})
}

// ActivateMemoryPolicyV4 is the only public transition path into the current
// memory policy. ExpectedFrom and the material capability acknowledgements are
// checked before pipeline registration and again inside the Canonical writer
// transaction so a stale caller cannot downgrade or bypass the transition.
func (a *Application) ActivateMemoryPolicyV4(
	ctx context.Context,
	options ActivateMemoryPolicyV4Options,
) (domain.MemoryPolicyActivationResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	resident, err := a.repository.Resident(ctx, options.ResidentID)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if resident.Status != "active" {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("app: memory policy activation requires active resident")
	}
	current, err := parseResidentMemoryPolicy(resident)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if err := validateMemoryPolicyV4Preflight(current.Version, options); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if current.Version != memory.PolicyVersionV4 {
		if err := a.registerMemoryPolicyV4Pipelines(ctx); err != nil {
			return domain.MemoryPolicyActivationResult{}, err
		}
	}
	activation, err := a.activateMemoryPolicy(ctx, resident, memory.DefaultPolicyV4(), memoryPolicyActivationTransition{
		expectedFrom:                  options.ExpectedFrom,
		acknowledgeRecallEnable:       options.AcknowledgeRecallEnable,
		acknowledgeSelfTalkExtraction: options.AcknowledgeSelfTalkExtraction,
	})
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if err := a.rebuildMemoryPolicyProjections(ctx, options.ResidentID); err != nil {
		return activation, err
	}
	return activation, nil
}

func (a *Application) rebuildMemoryPolicyProjections(ctx context.Context, residentID canonical.ID) error {
	rebuilder, ok := a.commitNotifier.(memoryPolicyProjectionRebuilder)
	if !ok || rebuilder == nil {
		// Offline owner-admin callers can commit the explicit policy transition
		// without a live coordinator. They remain gated by service readiness
		// until their host performs the required rebuild before opening ingress.
		return nil
	}
	if err := rebuilder.RebuildAll(ctx, residentID); err != nil {
		return fmt.Errorf("app: rebuild Projections after memory-policy-v4 activation: %w", err)
	}
	return nil
}

func (a *Application) registerMemoryPolicyV4Pipelines(ctx context.Context) error {
	pipelineIDs, err := a.allocateIDs(8)
	if err != nil {
		return err
	}
	pipelines, err := domain.MemoryPipelineDefinitions(pipelineIDs)
	if err != nil {
		return err
	}
	if _, err := a.submit(ctx, domain.RegisterPipelineVersionsCommand(pipelines)); err != nil {
		return fmt.Errorf("app: register memory pipelines: %w", err)
	}
	autonomyIDs, err := a.allocateIDs(2)
	if err != nil {
		return err
	}
	autonomyPipelines, err := domain.AutonomyPipelineDefinitions(autonomyIDs)
	if err != nil {
		return err
	}
	if _, err := a.submit(ctx, domain.RegisterPipelineVersionsCommand(autonomyPipelines)); err != nil {
		return fmt.Errorf("app: register autonomy pipelines: %w", err)
	}
	return nil
}

func parseResidentMemoryPolicy(resident domain.ResidentSnapshot) (memory.Policy, error) {
	policy, _, err := memory.ParsePolicy([]byte(resident.MemoryPolicy))
	if err != nil {
		return memory.Policy{}, fmt.Errorf("app: parse active memory policy: %w", err)
	}
	return policy, nil
}

func validateMemoryPolicyV4Preflight(
	current memory.PolicyVersion,
	options ActivateMemoryPolicyV4Options,
) error {
	if err := options.ExpectedFrom.Validate(); err != nil {
		return fmt.Errorf("app: --from is required for memory-policy-v4 activation: %w", err)
	}
	if options.ExpectedFrom != current {
		return fmt.Errorf("app: memory policy expected-from mismatch: expected %s, current %s",
			options.ExpectedFrom, current)
	}
	switch current {
	case memory.PolicyVersionV1:
		if !options.AcknowledgeRecallEnable || !options.AcknowledgeSelfTalkExtraction {
			return fmt.Errorf("app: memory-policy-v1 to v4 requires recall-enable and self-talk-extraction acknowledgements")
		}
	case memory.PolicyVersionV2:
		if !options.AcknowledgeSelfTalkExtraction {
			return fmt.Errorf("app: memory-policy-v2 to v4 requires self-talk-extraction acknowledgement")
		}
	case memory.PolicyVersionV3, memory.PolicyVersionV4:
		return nil
	default:
		return fmt.Errorf("app: unsupported active memory policy %q", current)
	}
	return nil
}

type memoryPolicyActivationTransition struct {
	expectedFrom                  memory.PolicyVersion
	acknowledgeRecallEnable       bool
	acknowledgeSelfTalkExtraction bool
}

func (a *Application) activateMemoryPolicy(
	ctx context.Context,
	resident domain.ResidentSnapshot,
	policy memory.Policy,
	transition memoryPolicyActivationTransition,
) (domain.MemoryPolicyActivationResult, error) {
	definition, err := policy.CanonicalJSON()
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	content, err := a.newContent(resident.ResidentID, "memory_policy_text", definition.Bytes(), "resident_only")
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	result, err := a.submitWithContent(ctx, domain.ActivateMemoryPolicyCommand(domain.ActivateMemoryPolicy{
		ResidentID: resident.ResidentID, OwnerPrincipalID: resident.OwnerPrincipalID,
		RevisionID: ids[0], ActivationID: ids[1], Content: content,
		ExpectedFrom:                  transition.expectedFrom,
		AcknowledgeRecallEnable:       transition.acknowledgeRecallEnable,
		AcknowledgeSelfTalkExtraction: transition.acknowledgeSelfTalkExtraction,
	}), []domain.Content{content})
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	activation, ok := result.Value.(domain.MemoryPolicyActivationResult)
	if !ok {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("app: memory policy activation returned an unexpected value")
	}
	activation.Changed = !result.Commit.CommitID.IsZero()
	return activation, nil
}
