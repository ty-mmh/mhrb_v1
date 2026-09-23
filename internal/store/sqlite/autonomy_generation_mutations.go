package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

func (u *canonicalUoW) PrepareAutonomousGeneration(
	ctx context.Context,
	value domain.PrepareAutonomousGeneration,
) error {
	if err := u.requireResidentScope(value.Generation.ResidentID); err != nil {
		return err
	}
	if err := u.requireActiveResident(ctx, value.Generation.ResidentID); err != nil {
		return err
	}
	if err := u.requireOperationallySelectedResident(ctx, value.Generation.ResidentID); err != nil {
		return err
	}
	if err := u.validateAutonomousDecision(ctx, value.Generation.Purpose, value.Trigger,
		value.Policy, value.ProjectionMaxStaleness, value.ProjectionEvidence, value.Usages,
		value.MaxAttempts, true, false); err != nil {
		return err
	}
	for _, revision := range []struct {
		id    canonical.ID
		class string
	}{
		{value.Generation.PrinciplesRevisionID, "principles"},
		{value.Generation.PersonaRevisionID, "persona"},
		{value.Generation.MemoryPolicyRevisionID, "memory_policy"},
	} {
		if err := u.requireActiveRevision(ctx, value.Generation.ResidentID, revision.id, revision.class); err != nil {
			return err
		}
	}
	purpose := value.Generation.Purpose.Effective()
	pipelineKind, pipelineVersion := autonomousPipelineContract(purpose)
	if err := u.requireExactMemoryPipeline(ctx, value.Generation.PipelineVersionID, pipelineKind, pipelineVersion); err != nil {
		return err
	}
	if err := u.validateAutonomousInputs(ctx, value); err != nil {
		return err
	}
	for _, input := range value.Generation.Inputs {
		if err := u.validateGenerationSource(ctx, value.Generation, input, false); err != nil {
			return err
		}
	}
	if err := u.insertGenerationRun(ctx, value.Generation); err != nil {
		return err
	}
	m := u.metadata
	for _, input := range value.Generation.Inputs {
		if err := u.insertContent(ctx, input.Content); err != nil {
			return err
		}
		var sourceID any
		if input.SourceID != nil {
			sourceID = input.SourceID.String()
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
			source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.ID.String(), m.CommitID.String(),
			value.Generation.RunID.String(), input.Ordinal, input.Role, input.SourceType, sourceID,
			input.InclusionMode, input.Content.ID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("sqlite: insert autonomous generation input %d: %w", input.Ordinal, err)
		}
	}
	for _, usage := range value.Usages {
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_usages(
			claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
			usage_type, ordinal, memory_policy_revision_id, exclusion_reason,
			detection_method, detection_confidence, detected_by_run_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, NULL, ?, 'prompt_included', ?, ?, NULL, NULL, NULL, NULL, ?, ?)`,
			usage.ID.String(), m.CommitID.String(), usage.ClaimID.String(), value.Generation.RunID.String(),
			usage.Ordinal.Int64(), value.Generation.MemoryPolicyRevisionID.String(),
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("sqlite: insert autonomous prompt usage %s: %w", usage.ClaimID, err)
		}
	}
	return u.insertOutcome(ctx, value.Generation.RunningOutcomeID, value.Generation.RunID, 1,
		"running", nil, nil, nil, 0, "")
}

func (u *canonicalUoW) LandAutonomousEvent(
	ctx context.Context,
	value domain.LandAutonomousEvent,
) (domain.Event, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireOperationallySelectedResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.Event{}, err
	}
	if state != "running" || latestAttempt != value.AttemptNo {
		return domain.Event{}, fmt.Errorf("sqlite: autonomous landing must terminate running attempt %d; got %s/%d",
			latestAttempt, state, value.AttemptNo)
	}
	var purposeRaw, key, pipelineRaw, policyRaw string
	var sessionRaw, recallRaw sql.NullString
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose, idempotency_key, pipeline_version_id,
		memory_policy_revision_id, sessionization_policy_version_id, recall_run_id
		FROM generation_runs WHERE generation_run_id = ? AND resident_id = ?`,
		value.RunID.String(), value.ResidentID.String()).Scan(
		&purposeRaw, &key, &pipelineRaw, &policyRaw, &sessionRaw, &recallRaw,
	); err != nil {
		return domain.Event{}, fmt.Errorf("sqlite: load autonomous generation envelope: %w", err)
	}
	purpose := domain.GenerationPurpose(purposeRaw)
	if purpose != domain.GenerationPurposeSelfTalk && purpose != domain.GenerationPurposeOutboundInitiative {
		return domain.Event{}, errors.New("sqlite: autonomous landing targets a different generation purpose")
	}
	wantKey, err := value.Trigger.IdempotencyKey(string(value.Policy.Version), value.ResidentID)
	if err != nil || key != wantKey || sessionRaw.Valid || recallRaw.Valid {
		return domain.Event{}, errors.New("sqlite: autonomous landing envelope does not match its trigger")
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		return domain.Event{}, err
	}
	pipelineKind, pipelineVersion := autonomousPipelineContract(purpose)
	if err := u.requireExactMemoryPipeline(ctx, pipelineID, pipelineKind, pipelineVersion); err != nil {
		return domain.Event{}, err
	}
	memoryPolicyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return domain.Event{}, err
	}
	if err := u.requireActiveRevision(ctx, value.ResidentID, memoryPolicyID, "memory_policy"); err != nil {
		return domain.Event{}, err
	}
	usages, err := u.autonomousRunUsages(ctx, value.RunID)
	if err != nil {
		return domain.Event{}, err
	}
	persistedTrigger, persistedPolicy, persistedMaxStaleness, persistedEvidence, err := u.autonomousRunProjectionEvidence(ctx, value.RunID)
	if err != nil {
		return domain.Event{}, err
	}
	wantIdentity, _ := value.Trigger.StableIdentity()
	gotIdentity, _ := persistedTrigger.StableIdentity()
	if wantIdentity != gotIdentity || !equalAutonomousProjectionEvidence(persistedEvidence, value.ProjectionEvidence) ||
		!equalAutonomyPolicy(persistedPolicy, value.Policy) || persistedMaxStaleness != value.ProjectionMaxStaleness {
		return domain.Event{}, errors.New("sqlite: autonomous landing does not match its frozen runtime projection")
	}
	if purpose == domain.GenerationPurposeOutboundInitiative {
		if persistedEvidence == nil {
			return domain.Event{}, errors.New("sqlite: initiative run has no frozen Projection evidence")
		}
		if err := persistedEvidence.Validate(value.ResidentID, memoryPolicyID); err != nil {
			return domain.Event{}, err
		}
	}
	preemptedRetry, err := u.autonomousRunWasForegroundPreempted(ctx, value.RunID, value.AttemptNo)
	if err != nil {
		return domain.Event{}, err
	}
	if err := u.validateAutonomousDecision(ctx, purpose, value.Trigger, persistedPolicy,
		persistedMaxStaleness, persistedEvidence, usages, value.MaxAttempts, false, preemptedRetry); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireResidentPrincipal(ctx, value.ResidentID, value.ResidentPrincipalID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.Event{}, err
	}
	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.Event{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "succeeded", &value.Output.ID,
		value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.Event{}, err
	}
	runID := value.RunID
	spec := eventSpec{
		ID: value.EventID, ResidentID: value.ResidentID, Ingress: "resident_runtime",
		ActorPrincipalID: value.ResidentPrincipalID, GenerationRunID: &runID,
		OccurredAt: value.OccurredAt, OccurredTZ: value.OccurredTZ, Content: value.Output,
	}
	visibility := "internal"
	if purpose == domain.GenerationPurposeSelfTalk {
		spec.Type = "self_talk"
		spec.Visibility = visibility
	} else {
		visibility = "conversation"
		target := value.OwnerPrincipalID
		spec.Type = "outbound_initiative"
		spec.Visibility = visibility
		spec.DeliveryScreen = true
		spec.TargetPrincipalID = &target
	}
	event, err := u.makeEvent(ctx, spec)
	if err != nil {
		return domain.Event{}, err
	}
	if err := u.insertEvent(ctx, event, visibility, spec.DeliveryScreen, false, "resident_runtime"); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

func (u *canonicalUoW) CancelAutonomousGeneration(
	ctx context.Context,
	value domain.CancelAutonomousGeneration,
) error {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	purpose, err := u.generationRunPurpose(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	if purpose != domain.GenerationPurposeSelfTalk && purpose != domain.GenerationPurposeOutboundInitiative {
		return errors.New("sqlite: autonomous cancellation targets a different generation purpose")
	}
	attempt, state, previousError, err := u.latestOutcomeWithError(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	code, err := generation.ParseOutcomeErrorCode(value.ErrorClass)
	if err != nil || code.Class() != generation.ErrorResidentInactive &&
		code.Class() != generation.ErrorSourceContentErased {
		return errors.New("sqlite: invalid autonomous cancellation error class")
	}
	if code.Class() == generation.ErrorSourceContentErased && attempt != value.AttemptNo {
		return fmt.Errorf("sqlite: erased-source cancellation attempt %d conflicts with latest attempt %d", value.AttemptNo, attempt)
	}
	switch state {
	case "running":
		return u.insertOutcome(ctx, value.CancelledOutcomeID, value.RunID, attempt,
			"cancelled", nil, nil, nil, 0, code.String())
	case "failed", "cancelled":
		previous, err := generation.ParseOutcomeErrorCode(previousError)
		if err != nil {
			return err
		}
		if code.Class() == generation.ErrorSourceContentErased {
			if previous.String() == code.String() {
				return canonical.ErrNoMutation
			}
			return errors.New("sqlite: erased-source cancellation conflicts with existing autonomous terminal outcome")
		}
		if !previous.Retryable() {
			return canonical.ErrNoMutation
		}
		if attempt == math.MaxInt64 {
			return errors.New("sqlite: autonomous cancellation attempt overflow")
		}
		attempt++
		if err := u.insertOutcome(ctx, value.RunningOutcomeID, value.RunID, attempt,
			"running", nil, nil, nil, 0, ""); err != nil {
			return err
		}
		return u.insertOutcome(ctx, value.CancelledOutcomeID, value.RunID, attempt,
			"cancelled", nil, nil, nil, 0, code.String())
	case "succeeded":
		if code.Class() == generation.ErrorSourceContentErased {
			return errors.New("sqlite: erased-source cancellation conflicts with succeeded autonomous work")
		}
		return canonical.ErrNoMutation
	default:
		return fmt.Errorf("sqlite: unknown autonomous cancellation state %q", state)
	}
}

func autonomousPipelineContract(purpose domain.GenerationPurpose) (string, string) {
	if purpose == domain.GenerationPurposeSelfTalk {
		return "self_talk", domain.SelfTalkPipelineVersion
	}
	return "outbound_initiative", domain.OutboundInitiativePipelineVersion
}

func (u *canonicalUoW) validateAutonomousDecision(
	ctx context.Context,
	purpose domain.GenerationPurpose,
	trigger autonomy.Trigger,
	policy autonomy.Policy,
	maxStaleness time.Duration,
	evidence *domain.AutonomousProjectionEvidence,
	usages []domain.AutonomousClaimUsage,
	maxAttempts int,
	requireCurrentProjection bool,
	allowPreemptedIdle bool,
) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	residentID, ok := u.metadata.Scope.ResidentID()
	if !ok {
		return errors.New("sqlite: autonomous decision requires resident scope")
	}
	if purpose == domain.GenerationPurposeSelfTalk {
		if err := u.requireAutonomousSelfTalkEventTimePolicy(ctx, residentID, trigger); err != nil {
			return err
		}
	}
	snapshot, err := u.autonomySnapshotForMutation(ctx, residentID, policy, maxAttempts)
	if err != nil {
		return err
	}
	if purpose == domain.GenerationPurposeOutboundInitiative &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV2) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV3) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV4) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV5) {
		return errors.New("sqlite: outbound initiative requires active memory-policy-v2, memory-policy-v3, memory-policy-v4, or memory-policy-v5")
	}
	requiredClaims, projectionAvailable, err := u.validateAutonomousTrigger(
		ctx, residentID, purpose, trigger, snapshot, maxStaleness, evidence, requireCurrentProjection,
		allowPreemptedIdle,
	)
	if err != nil {
		return err
	}
	snapshot.ProjectionAvailable = projectionAvailable
	if err := matchAutonomousUsageClaims(requiredClaims, usages); err != nil {
		return err
	}
	timing := autonomyTiming(snapshot, u.metadata.CommittedAt.Time())
	var decision autonomy.Decision
	if purpose == domain.GenerationPurposeSelfTalk {
		decision = autonomy.DecideSelfTalk(policy, snapshot, trigger, timing)
	} else if purpose == domain.GenerationPurposeOutboundInitiative {
		decision = autonomy.DecideInitiative(policy, snapshot, trigger, timing)
	} else {
		return errors.New("sqlite: non-autonomous purpose reached autonomous decision validator")
	}
	if !decision.Eligible {
		return fmt.Errorf("sqlite: autonomous generation blocked: %s", decision.BlockingReason)
	}
	return nil
}

func (u *canonicalUoW) autonomySnapshotForMutation(
	ctx context.Context,
	residentID canonical.ID,
	policy autonomy.Policy,
	maxAttempts int,
) (autonomy.Snapshot, error) {
	snapshot := autonomy.Snapshot{
		CapturedHead: canonical.Head{Exists: true, CommitSeq: u.metadata.CommitSeq, CommittedAt: u.metadata.CommittedAt},
		ResidentID:   residentID,
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT status.to_status
		FROM resident_status_transitions status
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = status.canonical_commit_id
		WHERE status.resident_id = ? ORDER BY commit_row.commit_seq DESC LIMIT 1`, residentID.String()).Scan(
		&snapshot.ResidentStatus,
	); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: load autonomy resident status: %w", err)
	}
	var policyContent []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, residentID.String()).Scan(
		&policyContent,
	); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: load autonomy memory policy: %w", err)
	}
	parsedPolicy, _, err := memory.ParsePolicy(policyContent)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	snapshot.MemoryPolicyVersion = string(parsedPolicy.Version)
	for _, item := range []struct {
		eventType string
		target    **autonomy.EventPoint
	}{
		{"user_message", &snapshot.LastUser},
		{"self_talk", &snapshot.LastSelfTalk},
		{"outbound_initiative", &snapshot.LastInitiative},
	} {
		var rawID string
		var seq, recordedAt int64
		err := u.tx.QueryRowContext(ctx, `SELECT event_id, seq, recorded_at FROM events
			WHERE resident_id = ? AND event_type = ? ORDER BY seq DESC LIMIT 1`, residentID.String(), item.eventType).Scan(
			&rawID, &seq, &recordedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return autonomy.Snapshot{}, err
		}
		id, err := canonical.ParseID(rawID)
		if err != nil {
			return autonomy.Snapshot{}, err
		}
		parsedSeq, err := canonical.NewSeq(seq)
		if err != nil {
			return autonomy.Snapshot{}, err
		}
		*item.target = &autonomy.EventPoint{ID: id, Seq: parsedSeq, RecordedAt: canonical.Instant(recordedAt)}
	}
	lastUserSeq := int64(0)
	if snapshot.LastUser != nil {
		lastUserSeq = snapshot.LastUser.Seq.Int64()
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'self_talk' AND seq > ?`, residentID.String(), lastUserSeq).Scan(
		&snapshot.ConsecutiveSelfTalk,
	); err != nil {
		return autonomy.Snapshot{}, err
	}
	location, err := policy.Location()
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	hourStart, hourEnd, dayStart, dayEnd := autonomy.CalendarBounds(u.metadata.CommittedAt.Time(), location)
	for _, count := range []struct {
		eventType string
		start     time.Time
		end       time.Time
		target    *int64
	}{
		{"self_talk", hourStart, hourEnd, &snapshot.SelfTalkHourCount},
		{"self_talk", dayStart, dayEnd, &snapshot.SelfTalkDayCount},
		{"outbound_initiative", hourStart, hourEnd, &snapshot.InitiativeHourCount},
		{"outbound_initiative", dayStart, dayEnd, &snapshot.InitiativeDayCount},
	} {
		if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
			WHERE resident_id = ? AND event_type = ? AND recorded_at >= ? AND recorded_at < ?`,
			residentID.String(), count.eventType, hourMicros(count.start), hourMicros(count.end)).Scan(count.target); err != nil {
			return autonomy.Snapshot{}, err
		}
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(recorded_at), 0) FROM events WHERE resident_id = ?`,
		residentID.String()).Scan(&snapshot.LatestRecordedAt); err != nil {
		return autonomy.Snapshot{}, err
	}
	foregroundPending, err := foregroundDialoguePending(
		ctx, u.tx, residentID, u.metadata.CommitSeq.Int64(), maxAttempts,
	)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	snapshot.ForegroundPending = foregroundPending
	return snapshot, nil
}

func hourMicros(value time.Time) int64 { return value.UnixMicro() }

func autonomyTiming(snapshot autonomy.Snapshot, now time.Time) autonomy.EvaluationTime {
	result := autonomy.EvaluationTime{WallNow: now}
	set := func(point *autonomy.EventPoint) *time.Duration {
		if point == nil {
			return nil
		}
		elapsed := now.Sub(point.RecordedAt.Time())
		return &elapsed
	}
	result.SinceLastUser = set(snapshot.LastUser)
	result.SinceLastSelfTalk = set(snapshot.LastSelfTalk)
	result.SinceLastInitiative = set(snapshot.LastInitiative)
	return result
}

func matchAutonomousUsageClaims(required []canonical.ID, usages []domain.AutonomousClaimUsage) error {
	want := make(map[canonical.ID]struct{}, len(required))
	for _, claimID := range required {
		want[claimID] = struct{}{}
	}
	if len(want) != len(usages) {
		return errors.New("sqlite: autonomous trigger claim/usage count mismatch")
	}
	for _, usage := range usages {
		if _, exists := want[usage.ClaimID]; !exists {
			return fmt.Errorf("sqlite: claim %s is not required by the autonomous trigger", usage.ClaimID)
		}
		delete(want, usage.ClaimID)
	}
	if len(want) != 0 {
		return errors.New("sqlite: autonomous trigger claim usage is incomplete")
	}
	return nil
}

func (u *canonicalUoW) validateAutonomousTrigger(
	ctx context.Context,
	residentID canonical.ID,
	purpose domain.GenerationPurpose,
	trigger autonomy.Trigger,
	snapshot autonomy.Snapshot,
	maxStaleness time.Duration,
	evidence *domain.AutonomousProjectionEvidence,
	requireCurrentProjection bool,
	allowPreemptedIdle bool,
) ([]canonical.ID, bool, error) {
	if err := trigger.Validate(); err != nil {
		return nil, false, err
	}
	switch trigger.Kind {
	case autonomy.TriggerIdle:
		identityMatches := snapshot.LastUser != nil && trigger.SourceID == snapshot.LastUser.ID &&
			trigger.Ordinal == snapshot.ConsecutiveSelfTalk+1
		if !identityMatches && allowPreemptedIdle && purpose == domain.GenerationPurposeSelfTalk &&
			trigger.Ordinal == snapshot.ConsecutiveSelfTalk+1 {
			var sourceType string
			if err := u.tx.QueryRowContext(ctx, `SELECT event_type FROM events
				WHERE event_id = ? AND resident_id = ?`, trigger.SourceID.String(), residentID.String()).Scan(&sourceType); err != nil {
				return nil, false, err
			}
			identityMatches = sourceType == "user_message"
		}
		if purpose != domain.GenerationPurposeSelfTalk || !identityMatches {
			return nil, false, errors.New("sqlite: idle trigger no longer matches Canonical counters")
		}
		return nil, true, nil
	case autonomy.TriggerMetaDependencyStatusChange:
		if purpose != domain.GenerationPurposeSelfTalk {
			return nil, false, errors.New("sqlite: dependency reevaluation requires self-talk purpose")
		}
		directID := trigger.RelatedClaimIDs[0]
		var dependencyRaw string
		var statusCommit, dependencyCommit int64
		if err := u.tx.QueryRowContext(ctx, `SELECT dependency.dependency_claim_id,
			status_commit.commit_seq, dependency_commit.commit_seq
			FROM claim_status_transitions status
			JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
			JOIN claim_stage_transition_dependencies dependency ON dependency.dependency_claim_id = status.claim_id
		JOIN canonical_commits dependency_commit ON dependency_commit.canonical_commit_id = dependency.canonical_commit_id
		JOIN claim_stage_transitions stage ON stage.stage_transition_id = dependency.stage_transition_id
		JOIN claims direct ON direct.claim_id = stage.claim_id
		JOIN claims dependency_claim ON dependency_claim.claim_id = dependency.dependency_claim_id
		JOIN content_objects direct_content ON direct_content.content_id = direct.statement_content_id
		JOIN content_objects dependency_content ON dependency_content.content_id = dependency_claim.statement_content_id
		WHERE status.status_transition_id = ? AND stage.claim_id = ?
		  AND stage.to_stage = 'settled' AND direct.owner_resident_id = ? AND direct.kind = 'direct'
		  AND direct_content.erasure_state = 'present' AND direct.statement_hash IS NOT NULL
		  AND dependency_content.erasure_state = 'present' AND dependency_claim.statement_hash IS NOT NULL`,
			trigger.SourceID.String(), directID.String(), residentID.String()).Scan(
			&dependencyRaw, &statusCommit, &dependencyCommit,
		); err != nil {
			return nil, false, fmt.Errorf("sqlite: resolve dependency reevaluation trigger: %w", err)
		}
		if statusCommit <= dependencyCommit {
			return nil, false, errors.New("sqlite: dependency status transition does not follow direct settlement")
		}
		dependencyID, err := canonical.ParseID(dependencyRaw)
		if err != nil {
			return nil, false, err
		}
		claims := []canonical.ID{directID, dependencyID}
		if err := u.requireAutonomousContextClaims(ctx, residentID, []canonical.ID{directID}, true); err != nil {
			return nil, false, err
		}
		if err := u.requireAutonomousExternalClaim(ctx, residentID, dependencyID); err != nil {
			return nil, false, err
		}
		return claims, true, nil
	case autonomy.TriggerSettledDirectConflict:
		if purpose != domain.GenerationPurposeSelfTalk {
			return nil, false, errors.New("sqlite: direct conflict reevaluation requires self-talk purpose")
		}
		var fromRaw, toRaw string
		if err := u.tx.QueryRowContext(ctx, `SELECT relation.from_claim_id, relation.to_claim_id
			FROM claim_relations relation
			JOIN claims source ON source.claim_id = relation.from_claim_id
			JOIN claims target ON target.claim_id = relation.to_claim_id
			JOIN content_objects source_content ON source_content.content_id = source.statement_content_id
			JOIN content_objects target_content ON target_content.content_id = target.statement_content_id
			WHERE relation.claim_relation_id = ? AND relation.relation_type = 'contradicts'
			  AND source.owner_resident_id = ? AND target.owner_resident_id = ?
			  AND source.kind = 'direct' AND target.kind = 'direct'
			  AND source_content.erasure_state = 'present' AND source.statement_hash IS NOT NULL
			  AND target_content.erasure_state = 'present' AND target.statement_hash IS NOT NULL`, trigger.SourceID.String(),
			residentID.String(), residentID.String()).Scan(&fromRaw, &toRaw); err != nil {
			return nil, false, fmt.Errorf("sqlite: resolve conflict reevaluation trigger: %w", err)
		}
		fromID, err := canonical.ParseID(fromRaw)
		if err != nil {
			return nil, false, err
		}
		toID, err := canonical.ParseID(toRaw)
		if err != nil {
			return nil, false, err
		}
		if !sameClaimSet(trigger.RelatedClaimIDs, []canonical.ID{fromID, toID}) {
			return nil, false, errors.New("sqlite: conflict trigger claim identity mismatch")
		}
		claims := []canonical.ID{fromID, toID}
		if err := u.requireAutonomousContextClaims(ctx, residentID, claims, true); err != nil {
			return nil, false, err
		}
		return claims, true, nil
	case autonomy.TriggerFutureToCurrent, autonomy.TriggerVolatileAging:
		if purpose != domain.GenerationPurposeOutboundInitiative {
			return nil, false, errors.New("sqlite: temporal trigger requires outbound initiative purpose")
		}
		claimID := trigger.SourceID
		if err := u.requireAutonomousContextClaims(ctx, residentID, []canonical.ID{claimID}, false); err != nil {
			return nil, false, err
		}
		if evidence == nil {
			return []canonical.ID{claimID}, false, errors.New("sqlite: initiative trigger has no frozen Projection evidence")
		}
		if err := u.validateInitiativeProjectionEvidence(ctx, residentID, snapshot, *evidence,
			maxStaleness, requireCurrentProjection); err != nil {
			return []canonical.ID{claimID}, false, err
		}
		boundary, err := u.initiativeBoundary(ctx, residentID, claimID, trigger.Kind)
		if err != nil {
			return nil, false, err
		}
		if boundary != trigger.Boundary {
			return nil, false, errors.New("sqlite: initiative temporal boundary changed")
		}
		if requireCurrentProjection {
			relation, err := u.initiativeProjectionRelation(ctx, residentID, claimID)
			if err != nil {
				return nil, false, err
			}
			if trigger.Kind == autonomy.TriggerFutureToCurrent && relation != "current" {
				return nil, false, errors.New("sqlite: future-to-current trigger is not current")
			}
			if trigger.Kind == autonomy.TriggerVolatileAging && relation != "stale_unknown" {
				return nil, false, errors.New("sqlite: volatile-aging trigger is not stale_unknown")
			}
		} else {
			if err := u.requireNoInitiativeClaimChangeAfter(ctx, claimID, evidence.CapturedHead); err != nil {
				return nil, false, err
			}
			eligible, err := u.currentAutonomousProjectionEligible(ctx, residentID, trigger, maxStaleness)
			if err != nil {
				return nil, false, err
			}
			if !eligible {
				return nil, false, errors.New("sqlite: initiative live Projection relation is no longer eligible")
			}
		}
		return []canonical.ID{claimID}, true, nil
	default:
		return nil, false, errors.New("sqlite: unsupported autonomous trigger")
	}
}

func (u *canonicalUoW) autonomousRunWasForegroundPreempted(
	ctx context.Context,
	runID canonical.ID,
	attemptNo int64,
) (bool, error) {
	if attemptNo <= 1 {
		return false, nil
	}
	var residentRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id FROM generation_runs WHERE generation_run_id = ?`, runID.String()).Scan(&residentRaw); err != nil {
		return false, err
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		return false, err
	}
	summary, err := (generationOutcomeRepository{}).Summary(ctx, u.tx, runID, residentID)
	if err != nil {
		return false, err
	}
	_ = attemptNo
	return summary.HasForegroundPreemptedAttempt, nil
}

func (u *canonicalUoW) requireAutonomousExternalClaim(
	ctx context.Context,
	residentID, claimID canonical.ID,
) error {
	if _, err := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID); err != nil {
		return fmt.Errorf("sqlite: resolve autonomous external claim %s: %w", claimID, err)
	}
	var external int
	if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		JOIN claim_evidence evidence ON evidence.claim_id = claim.claim_id
		JOIN events event ON event.event_id = evidence.event_id
		WHERE claim.claim_id = ? AND claim.owner_resident_id = ?
		 AND content.erasure_state = 'present' AND claim.statement_hash IS NOT NULL
		 AND evidence.polarity = 'support' AND event.event_type <> 'self_talk'
	)`, claimID.String(), residentID.String()).Scan(&external); err != nil {
		return err
	}
	if external == 0 {
		return fmt.Errorf("sqlite: autonomous context claim %s has only self-talk support", claimID)
	}
	return nil
}

func sameClaimSet(left, right []canonical.ID) bool {
	if len(left) != len(right) {
		return false
	}
	want := make(map[canonical.ID]int, len(left))
	for _, id := range left {
		want[id]++
	}
	for _, id := range right {
		want[id]--
		if want[id] < 0 {
			return false
		}
	}
	for _, count := range want {
		if count != 0 {
			return false
		}
	}
	return true
}

func (u *canonicalUoW) requireAutonomousContextClaims(
	ctx context.Context,
	residentID canonical.ID,
	claimIDs []canonical.ID,
	requireExternalSupport bool,
) error {
	for _, claimID := range claimIDs {
		if _, err := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID); err != nil {
			return fmt.Errorf("sqlite: resolve autonomous context claim %s: %w", claimID, err)
		}
		var kind, stage, status, scope string
		var external int
		err := u.tx.QueryRowContext(ctx, `SELECT COALESCE(claim.kind, ''),
			(SELECT transition.to_stage FROM claim_stage_transitions transition
			 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
			 WHERE transition.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, transition.stage_transition_id DESC LIMIT 1),
			COALESCE((SELECT transition.to_status FROM claim_status_transitions transition
			 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
			 WHERE transition.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, transition.status_transition_id DESC LIMIT 1), 'active'),
			(SELECT assertion.view_scope FROM claim_view_scope_assertions assertion
			 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
			 WHERE assertion.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, assertion.view_scope_assertion_id DESC LIMIT 1),
			EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
			 WHERE evidence.claim_id = claim.claim_id AND evidence.polarity = 'support' AND event.event_type <> 'self_talk')
			FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			WHERE claim.claim_id = ? AND claim.owner_resident_id = ?
			  AND content.erasure_state = 'present' AND claim.statement_hash IS NOT NULL`,
			claimID.String(), residentID.String()).Scan(&kind, &stage, &status, &scope, &external)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: autonomous context claim %s is erased or ineligible", domain.ErrClaimSourceIneligible, claimID)
			}
			return fmt.Errorf("sqlite: resolve autonomous context claim %s: %w", claimID, err)
		}
		if stage != "settled" || status != "active" {
			return fmt.Errorf("sqlite: autonomous context claim %s is not active and settled", claimID)
		}
		// No autonomous purpose may bootstrap its own context from a claim whose
		// entire support chain is private self-talk. Initiative applies this as
		// defense in depth even though settled Projection state should already
		// reject such maturation.
		if external == 0 {
			return fmt.Errorf("sqlite: autonomous context claim %s has only self-talk support", claimID)
		}
		if !requireExternalSupport && (kind != "direct" || scope != "resident_ui") {
			return fmt.Errorf("sqlite: initiative claim %s is not direct resident_ui", claimID)
		}
	}
	return nil
}

func (u *canonicalUoW) validateInitiativeProjectionEvidence(
	ctx context.Context,
	residentID canonical.ID,
	snapshot autonomy.Snapshot,
	evidence domain.AutonomousProjectionEvidence,
	maxStaleness time.Duration,
	requireCurrent bool,
) error {
	if maxStaleness <= 0 || u.metadata.CommitSeq.Int64() <= 1 {
		return errors.New("sqlite: initiative Projection evidence has no capturable head")
	}
	if err := evidence.CapturedHead.Validate(); err != nil {
		return err
	}
	head := evidence.CapturedHead.Int64()
	var activePolicyRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		  AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, residentID.String(), head).Scan(
		&activePolicyRaw,
	); err != nil {
		return err
	}
	activePolicyID, err := canonical.ParseID(activePolicyRaw)
	if err != nil {
		return err
	}
	if err := evidence.Validate(residentID, activePolicyID); err != nil {
		return err
	}
	if evidence.ClaimStates.AsOf > u.metadata.CommittedAt ||
		u.metadata.CommittedAt.Time().Sub(evidence.ClaimStates.AsOf.Time()) > maxStaleness {
		return errors.New("sqlite: frozen initiative Projection evidence is stale")
	}
	if requireCurrent {
		if evidence.CapturedHead.Int64() != u.metadata.CommitSeq.Int64()-1 {
			return errors.New("sqlite: initiative Projection evidence does not target the pre-prepare head")
		}
		claimWatermark, claimExists, err := readProjectionWatermark(ctx, u.tx, evidence.ClaimStates.ProjectionName, residentID)
		if err != nil || !claimExists {
			return errors.New("sqlite: claim-state Projection evidence is unavailable")
		}
		viewWatermark, viewExists, err := readProjectionWatermark(ctx, u.tx, evidence.ViewScope.ProjectionName, residentID)
		if err != nil || !viewExists {
			return errors.New("sqlite: view-scope Projection evidence is unavailable")
		}
		if !watermarksEqual(claimWatermark, evidence.ClaimStates) ||
			!watermarksEqual(viewWatermark, evidence.ViewScope) {
			return errors.New("sqlite: initiative Projection changed before prepare")
		}
	} else if snapshot.CapturedHead.CommitSeq <= evidence.CapturedHead {
		return errors.New("sqlite: initiative landing did not advance beyond its prepare capture")
	}
	return nil
}

func (u *canonicalUoW) autonomousRunProjectionEvidence(
	ctx context.Context,
	runID canonical.ID,
) (autonomy.Trigger, autonomy.Policy, time.Duration, *domain.AutonomousProjectionEvidence, error) {
	var content []byte
	err := u.tx.QueryRowContext(ctx, `SELECT blob.content
		FROM generation_run_inputs input
		JOIN content_objects object ON object.content_id = input.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE input.generation_run_id = ? AND input.source_type = 'runtime_projection'
		 AND input.inclusion_mode = 'runtime_projection'`, runID.String()).Scan(&content)
	if err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, fmt.Errorf("sqlite: load autonomous frozen runtime projection: %w", err)
	}
	return domain.ParseAutonomousRuntimeProjection(content)
}

func (u *canonicalUoW) validateAutonomousRetryStart(
	ctx context.Context,
	runID canonical.ID,
	attemptNo int64,
	maxAttempts int,
	purpose domain.GenerationPurpose,
) error {
	if maxAttempts < 1 {
		return errors.New("sqlite: autonomous retry max attempts must be positive")
	}
	trigger, policy, maxStaleness, evidence, err := u.autonomousRunProjectionEvidence(ctx, runID)
	if err != nil {
		return err
	}
	if purpose == domain.GenerationPurposeSelfTalk && !trigger.Kind.IsSelfTalk() ||
		purpose == domain.GenerationPurposeOutboundInitiative && !trigger.Kind.IsInitiative() {
		return errors.New("sqlite: autonomous retry purpose does not match its frozen trigger")
	}
	var residentRaw, policyRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, memory_policy_revision_id
		FROM generation_runs WHERE generation_run_id = ?`, runID.String()).Scan(&residentRaw, &policyRaw); err != nil {
		return fmt.Errorf("sqlite: load autonomous retry revisions: %w", err)
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		return err
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return err
	}
	summary, err := (generationOutcomeRepository{}).Summary(ctx, u.tx, runID, residentID)
	if err != nil {
		return fmt.Errorf("sqlite: classify autonomous retry history: %w", err)
	}
	classified, err := summary.Classify(int64(maxAttempts))
	if err != nil {
		return fmt.Errorf("sqlite: classify autonomous retry policy: %w", err)
	}
	if !classified.RetryEligible || classified.Latest.AttemptNo == math.MaxInt64 ||
		classified.Latest.AttemptNo+1 != attemptNo {
		return fmt.Errorf(
			"sqlite: autonomous retry attempt %d is not eligible under maxAttempts=%d after attempt %d",
			attemptNo, maxAttempts, classified.Latest.AttemptNo,
		)
	}
	if err := u.requireActiveRevision(ctx, residentID, policyID, "memory_policy"); err != nil {
		return err
	}
	usages, err := u.autonomousRunUsages(ctx, runID)
	if err != nil {
		return err
	}
	preempted, err := u.autonomousRunWasForegroundPreempted(ctx, runID, attemptNo)
	if err != nil {
		return err
	}
	if err := u.validateAutonomousDecision(ctx, purpose, trigger, policy, maxStaleness,
		evidence, usages, maxAttempts, false, preempted); err != nil {
		return fmt.Errorf("sqlite: autonomous retry is no longer eligible: %w", err)
	}
	return nil
}

func (u *canonicalUoW) currentAutonomousProjectionEligible(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
	maxStaleness time.Duration,
) (bool, error) {
	if err := trigger.Validate(); err != nil || !trigger.Kind.IsInitiative() ||
		maxStaleness <= 0 || u.metadata.CommitSeq.Int64() <= 1 {
		return false, nil
	}
	var policyRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, residentID.String()).Scan(
		&policyRaw,
	); err != nil {
		return false, err
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return false, err
	}
	claimWatermark, claimExists, err := readProjectionWatermark(
		ctx, u.tx, projection.ClaimStatesName, residentID,
	)
	if err != nil || !claimExists {
		return false, err
	}
	viewWatermark, viewExists, err := readProjectionWatermark(
		ctx, u.tx, projection.ClaimViewScopeCurrentName, residentID,
	)
	if err != nil || !viewExists {
		return false, err
	}
	evidence := domain.AutonomousProjectionEvidence{
		CapturedHead: claimWatermark.SourceCommitSeq, ClaimStates: claimWatermark, ViewScope: viewWatermark,
	}
	if evidence.Validate(residentID, policyID) != nil ||
		claimWatermark.SourceCommitSeq.Int64() > u.metadata.CommitSeq.Int64()-1 ||
		claimWatermark.AsOf > u.metadata.CommittedAt ||
		u.metadata.CommittedAt.Time().Sub(claimWatermark.AsOf.Time()) > maxStaleness {
		return false, nil
	}
	relation, err := u.initiativeProjectionRelation(ctx, residentID, trigger.SourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	wantRelation := "current"
	if trigger.Kind == autonomy.TriggerVolatileAging {
		wantRelation = "stale_unknown"
	}
	return relation == wantRelation, nil
}

func equalAutonomyPolicy(left, right autonomy.Policy) bool {
	// Runtime projection parsing already enforces a closed policy. Equality is
	// re-derived from the exact validated fields without relying on pointer or
	// slice identity.
	return left.Version == right.Version && left.Timezone == right.Timezone &&
		left.ScanInterval == right.ScanInterval && left.SelfTalk == right.SelfTalk &&
		left.Initiative.Enabled == right.Initiative.Enabled &&
		left.Initiative.MinimumInterval == right.Initiative.MinimumInterval &&
		left.Initiative.HourlyLimit == right.Initiative.HourlyLimit &&
		left.Initiative.DailyLimit == right.Initiative.DailyLimit &&
		left.Initiative.RecentUserSuppression == right.Initiative.RecentUserSuppression &&
		left.Initiative.QuietHours == right.Initiative.QuietHours &&
		sameTriggerKinds(left.Initiative.Triggers, right.Initiative.Triggers) && left.Retention == right.Retention
}

func sameTriggerKinds(left, right []autonomy.TriggerKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalAutonomousProjectionEvidence(
	left, right *domain.AutonomousProjectionEvidence,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.CapturedHead == right.CapturedHead &&
		watermarksEqual(left.ClaimStates, right.ClaimStates) &&
		watermarksEqual(left.ViewScope, right.ViewScope)
}

func (u *canonicalUoW) initiativeProjectionRelation(
	ctx context.Context,
	residentID, claimID canonical.ID,
) (string, error) {
	var relation, scope string
	if err := u.tx.QueryRowContext(ctx, `SELECT state.temporal_relation, scope.view_scope
		FROM claim_states state JOIN claim_view_scope_current scope
		  ON scope.resident_id = state.resident_id AND scope.claim_id = state.claim_id
		WHERE state.resident_id = ? AND state.claim_id = ?`, residentID.String(), claimID.String()).Scan(
		&relation, &scope,
	); err != nil {
		return "", err
	}
	if scope != "resident_ui" {
		return "", errors.New("sqlite: initiative Projection scope is not resident_ui")
	}
	return relation, nil
}

func (u *canonicalUoW) requireNoInitiativeClaimChangeAfter(
	ctx context.Context,
	claimID canonical.ID,
	frozenHead canonical.CommitSeq,
) error {
	var changes int
	if err := u.tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM claim_status_transitions row JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id WHERE row.claim_id = ? AND commit_row.commit_seq > ?) +
		(SELECT COUNT(*) FROM claim_stage_transitions row JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id WHERE row.claim_id = ? AND commit_row.commit_seq > ?) +
		(SELECT COUNT(*) FROM claim_view_scope_assertions row JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id WHERE row.claim_id = ? AND commit_row.commit_seq > ?) +
		(SELECT COUNT(*) FROM claim_validity_assertions row JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id WHERE row.claim_id = ? AND commit_row.commit_seq > ?) +
		(SELECT COUNT(*) FROM claim_evidence row JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id WHERE row.claim_id = ? AND commit_row.commit_seq > ?)`,
		claimID.String(), frozenHead.Int64(), claimID.String(), frozenHead.Int64(),
		claimID.String(), frozenHead.Int64(), claimID.String(), frozenHead.Int64(),
		claimID.String(), frozenHead.Int64()).Scan(&changes); err != nil {
		return err
	}
	if changes != 0 {
		return errors.New("sqlite: initiative claim changed after its frozen Projection capture")
	}
	return nil
}

func (u *canonicalUoW) initiativeBoundary(
	ctx context.Context,
	residentID, claimID canonical.ID,
	kind autonomy.TriggerKind,
) (canonical.Instant, error) {
	switch kind {
	case autonomy.TriggerFutureToCurrent:
		var claimRecordedAt, assertionRecordedAt int64
		var boundary sql.NullInt64
		if err := u.tx.QueryRowContext(ctx, `SELECT claim.recorded_at, assertion.valid_from, assertion.recorded_at
			FROM claim_validity_assertions assertion
			JOIN claims claim ON claim.claim_id = assertion.claim_id
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
			WHERE assertion.claim_id = ? AND claim.owner_resident_id = ?
			  AND content.erasure_state = 'present' AND claim.statement_hash IS NOT NULL
			ORDER BY commit_row.commit_seq DESC, assertion.validity_assertion_id DESC LIMIT 1`,
			claimID.String(), residentID.String()).Scan(&claimRecordedAt, &boundary, &assertionRecordedAt); err != nil {
			return 0, fmt.Errorf("sqlite: resolve future-to-current boundary: %w", err)
		}
		if !boundary.Valid || boundary.Int64 == 0 {
			return 0, errors.New("sqlite: future-to-current claim has no explicit valid_from")
		}
		if claimRecordedAt >= boundary.Int64 || assertionRecordedAt >= boundary.Int64 {
			return 0, errors.New("sqlite: future-to-current claim was not recorded while its boundary was future")
		}
		return canonical.Instant(boundary.Int64), nil
	case autonomy.TriggerVolatileAging:
		var temporalKind string
		var latestSupport sql.NullInt64
		if err := u.tx.QueryRowContext(ctx, `SELECT claim.temporal_kind,
			MAX(CASE WHEN evidence.polarity = 'support' THEN evidence.recorded_at END)
			FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			LEFT JOIN claim_evidence evidence ON evidence.claim_id = claim.claim_id
			WHERE claim.claim_id = ? AND claim.owner_resident_id = ?
			  AND content.erasure_state = 'present' AND claim.statement_hash IS NOT NULL
			GROUP BY claim.claim_id`,
			claimID.String(), residentID.String()).Scan(&temporalKind, &latestSupport); err != nil {
			return 0, fmt.Errorf("sqlite: resolve volatile-aging boundary: %w", err)
		}
		if temporalKind != "volatile" || !latestSupport.Valid {
			return 0, errors.New("sqlite: volatile-aging claim has no support anchor")
		}
		boundary := time.UnixMicro(latestSupport.Int64).Add(30 * 24 * time.Hour)
		return canonical.Instant(boundary.UnixMicro()), nil
	default:
		return 0, errors.New("sqlite: trigger has no initiative boundary")
	}
}

func (u *canonicalUoW) validateAutonomousInputs(ctx context.Context, value domain.PrepareAutonomousGeneration) error {
	revisionInputs := map[canonical.ID]bool{
		value.Generation.PrinciplesRevisionID:   false,
		value.Generation.PersonaRevisionID:      false,
		value.Generation.MemoryPolicyRevisionID: false,
	}
	runtimeInputs := 0
	claimUsages := make(map[canonical.ID]struct{}, len(value.Usages))
	for _, usage := range value.Usages {
		claimUsages[usage.ClaimID] = struct{}{}
	}
	for _, input := range value.Generation.Inputs {
		switch input.SourceType {
		case "resident_revision":
			if input.SourceID == nil {
				return errors.New("sqlite: autonomous resident definition has no revision")
			}
			if _, expected := revisionInputs[*input.SourceID]; !expected || revisionInputs[*input.SourceID] {
				return errors.New("sqlite: autonomous resident definitions are incomplete or duplicated")
			}
			var source []byte
			if err := u.tx.QueryRowContext(ctx, `SELECT blob.content
				FROM resident_revisions revision
				JOIN content_objects content ON content.content_id = revision.content_id
				JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
				 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
				WHERE revision.revision_id = ?`, input.SourceID.String()).Scan(&source); err != nil {
				return err
			}
			if !bytes.Equal(source, input.Content.Bytes) {
				return errors.New("sqlite: autonomous resident definition bytes differ from their revision")
			}
			revisionInputs[*input.SourceID] = true
		case "runtime_projection":
			if input.SourceID != nil || input.InclusionMode != "runtime_projection" {
				return errors.New("sqlite: invalid autonomous trigger projection input")
			}
			runtimeInputs++
		case "claim":
			if input.SourceID == nil || input.InclusionMode != "memory_recall" {
				return errors.New("sqlite: invalid autonomous claim input")
			}
			if _, expected := claimUsages[*input.SourceID]; !expected {
				return errors.New("sqlite: autonomous claim input lacks prompt usage")
			}
			eligible, err := loadEligibleClaimStatement(ctx, u.tx, value.Generation.ResidentID, *input.SourceID)
			if err != nil {
				return fmt.Errorf("sqlite: autonomous claim input %s: %w", *input.SourceID, err)
			}
			if !bytes.Equal(eligible.Statement, input.Content.Bytes) {
				return errors.New("sqlite: autonomous claim input bytes differ from its statement")
			}
		default:
			return fmt.Errorf("sqlite: autonomous generation rejects source type %q", input.SourceType)
		}
	}
	for _, included := range revisionInputs {
		if !included {
			return errors.New("sqlite: autonomous generation requires all active resident definitions")
		}
	}
	if runtimeInputs != 1 {
		return errors.New("sqlite: autonomous generation requires exactly one trigger projection")
	}
	return nil
}

func (u *canonicalUoW) autonomousRunUsages(ctx context.Context, runID canonical.ID) ([]domain.AutonomousClaimUsage, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT claim_usage_id, claim_id, ordinal FROM claim_usages
		WHERE generation_run_id = ? AND recall_run_id IS NULL AND usage_type = 'prompt_included'
		ORDER BY ordinal`, runID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.AutonomousClaimUsage
	for rows.Next() {
		var rawID, rawClaim string
		var ordinal int64
		if err := rows.Scan(&rawID, &rawClaim, &ordinal); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(rawID)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(rawClaim)
		if err != nil {
			return nil, err
		}
		parsedOrdinal, err := canonical.NewOrdinal(ordinal)
		if err != nil {
			return nil, err
		}
		result = append(result, domain.AutonomousClaimUsage{ID: id, ClaimID: claimID, Ordinal: parsedOrdinal})
	}
	return result, rows.Err()
}
