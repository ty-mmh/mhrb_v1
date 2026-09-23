package erasure

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

type IDAllocator interface{ New() (canonical.ID, error) }

type BlobReader interface {
	Open(context.Context, canonical.ID, canonical.Digest) (io.ReadCloser, error)
}

type PlanRequest struct {
	Scope                         string
	ResidentID                    canonical.ID
	ActorPrincipalID              canonical.ID
	ReasonCode                    string
	IntegrityPipelineVersionID    canonical.ID
	MemoryStatusPipelineVersionID canonical.ID
	RequestedContentIDs           []canonical.ID
}

type CaptureRequest struct {
	Scope                         string
	ResidentID                    canonical.ID
	ActorPrincipalID              canonical.ID
	IntegrityPipelineVersionID    canonical.ID
	MemoryStatusPipelineVersionID canonical.ID
	RequestedContentIDs           []canonical.ID
}

type ContentSnapshot struct {
	ContentID            canonical.ID
	ResidentID           canonical.ID
	ContentClass         string
	ErasurePolicy        string
	ErasureState         string
	Commitment           canonical.Digest
	BlobHash             *canonical.Digest
	SQLiteBlobValid      bool
	FilesystemBlobValid  bool
	SQLiteBlobStatus     string
	FilesystemBlobStatus string
}

type AliasSnapshot struct {
	ClaimID                      canonical.ID
	ResidentID                   canonical.ID
	StatementContentID           canonical.ID
	StatementHashPresent         bool
	ClaimStatementErasureEventID *canonical.ID
	ContentErasureEventID        *canonical.ID
	CanonicalCommitID            *canonical.ID
	CurrentStatus                string
	ExistingFinding              *ExistingFindingSnapshot
}

type ExistingFindingSnapshot struct {
	IntegrityFindingID          canonical.ID
	FindingKind                 string
	RuleCode                    string
	SourceContentErasureEventID *canonical.ID
	PipelineVersionID           canonical.ID
	Fingerprint                 canonical.Digest
}

type DirectReference struct {
	ContentID     canonical.ID
	RuleID        string
	ReferrerKind  string
	ReferrerID    string
	ReferrerField string
}

type LineageEdge struct {
	ParentContentID canonical.ID
	ChildContentID  canonical.ID
	RunID           canonical.ID
	SameLogicalBlob bool
	Valid           bool
}

type RunningWork struct {
	RunID      canonical.ID
	Purpose    string
	ContentIDs []canonical.ID
}

type ProjectionDefinition struct{ Name, Version string }

type Snapshot struct {
	HeadCommitID          canonical.ID
	HeadCommitSeq         canonical.CommitSeq
	ResidentStatus        string
	OwnerHuman            bool
	ActiveResidentID      *canonical.ID
	Contents              []ContentSnapshot
	Aliases               []AliasSnapshot
	DirectReferences      []DirectReference
	Lineage               []LineageEdge
	MandatoryWorkCount    int
	Running               []RunningWork
	ProjectionDefinitions []ProjectionDefinition
	Blockers              []Blocker
}

type SnapshotSource interface {
	CaptureErasureSnapshot(context.Context, CaptureRequest) (Snapshot, error)
}

// LogicalProvenanceSource is an optional production extension to SnapshotSource.
// It evaluates history-bearing references after the planner has closed the
// physical target set and assigned the provisional erasure event identities.
// Keeping this read behind the same base-head contract lets the SQLite
// implementation reuse the integrity gate evaluator without putting policy
// bytes or mutable database handles in Plan.
type LogicalProvenanceSource interface {
	CaptureErasureLogicalProvenance(context.Context, LogicalProvenanceRequest) (LogicalProvenanceSnapshot, error)
}

type LogicalProvenanceRequest struct {
	ResidentID        canonical.ID
	BaseHeadCommitID  canonical.ID
	BaseHeadCommitSeq canonical.CommitSeq
	Targets           []LogicalErasureTarget
}

type LogicalErasureTarget struct {
	ContentID      canonical.ID
	ErasureEventID canonical.ID
}

type LogicalReferenceSnapshot struct {
	ContentID       canonical.ID
	RuleID          string
	ReferrerKind    string
	ReferrerID      string
	ReferrerField   string
	AffectedClaimID *canonical.ID
	BreaksClaim     bool
}

type ProvenanceBreakSnapshot struct {
	ClaimID                     canonical.ID
	CurrentStatus               string
	SourceContentErasureEventID canonical.ID
}

type ExistingProvenanceFindingSnapshot struct {
	ClaimID       canonical.ID
	CurrentStatus string
	Finding       ExistingFindingSnapshot
}

type LogicalProvenanceSnapshot struct {
	References                  []LogicalReferenceSnapshot
	Breaks                      []ProvenanceBreakSnapshot
	ExistingFindingDependencies []ExistingProvenanceFindingSnapshot
	Blockers                    []Blocker
}

type Planner struct {
	Source SnapshotSource
	IDs    IDAllocator
}

func (planner Planner) Plan(ctx context.Context, request PlanRequest) (Plan, error) {
	if planner.Source == nil || planner.IDs == nil {
		return Plan{}, fmt.Errorf("erasure: planner source and IDs are required")
	}
	if request.Scope != ScopeContent && request.Scope != ScopeResident {
		return Plan{}, fmt.Errorf("%w: scope", ErrInvalidPlan)
	}
	for _, id := range []canonical.ID{request.ResidentID, request.ActorPrincipalID, request.IntegrityPipelineVersionID, request.MemoryStatusPipelineVersionID} {
		if err := id.Validate(); err != nil {
			return Plan{}, err
		}
	}
	if !reasonPattern.MatchString(request.ReasonCode) {
		return Plan{}, fmt.Errorf("%w: reason", ErrInvalidPlan)
	}
	roots := slices.Clone(request.RequestedContentIDs)
	slices.SortFunc(roots, func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
	for i, id := range roots {
		if err := id.Validate(); err != nil {
			return Plan{}, err
		}
		if i > 0 && id == roots[i-1] {
			return Plan{}, fmt.Errorf("%w: duplicate requested content", ErrInvalidPlan)
		}
	}
	if request.Scope == ScopeContent && len(roots) == 0 {
		return Plan{}, fmt.Errorf("%w: empty roots", ErrInvalidPlan)
	}
	if request.Scope == ScopeResident && len(roots) != 0 {
		return Plan{}, fmt.Errorf("%w: resident roots must be discovered", ErrInvalidPlan)
	}
	snapshot, err := planner.Source.CaptureErasureSnapshot(ctx, CaptureRequest{
		Scope: request.Scope, ResidentID: request.ResidentID, ActorPrincipalID: request.ActorPrincipalID,
		IntegrityPipelineVersionID: request.IntegrityPipelineVersionID, MemoryStatusPipelineVersionID: request.MemoryStatusPipelineVersionID,
		RequestedContentIDs: roots,
	})
	if err != nil {
		return Plan{}, err
	}
	return planner.build(ctx, request, roots, snapshot, nil)
}

// Decide applies one exact review round. An erase decision for a
// content_candidate promotes that candidate to a requested root and captures
// the resident snapshot again. Planned IDs for rows whose identity did not
// change are retained across rounds; newly discovered rows receive new IDs.
func (planner Planner) Decide(ctx context.Context, plan Plan, decisions []DecisionInput) (Plan, error) {
	if planner.Source == nil || planner.IDs == nil {
		return Plan{}, fmt.Errorf("erasure: planner source and IDs are required")
	}
	if err := validatePlan(plan, true); err != nil {
		return Plan{}, err
	}
	if len(plan.Blockers) != 0 {
		return Plan{}, ErrBlocked
	}
	decisionByID := make(map[string]string, len(decisions))
	for _, decision := range decisions {
		if decision.Decision != "retain" && decision.Decision != "erase" {
			return Plan{}, fmt.Errorf("%w: unknown decision", ErrInvalidPlan)
		}
		if _, exists := decisionByID[decision.ImpactID]; exists {
			return Plan{}, fmt.Errorf("%w: duplicate decision", ErrInvalidPlan)
		}
		decisionByID[decision.ImpactID] = decision.Decision
	}

	roots := make([]canonical.ID, 0, len(plan.RequestedContentIDs)+len(decisions))
	for _, raw := range plan.RequestedContentIDs {
		id, err := canonical.ParseID(raw)
		if err != nil {
			return Plan{}, err
		}
		roots = append(roots, id)
	}
	oldImpacts := make(map[string]Impact, len(plan.Impacts))
	promoted := make(map[string]struct{})
	for _, impact := range plan.Impacts {
		oldImpacts[impact.ImpactID] = impact
		decision, supplied := decisionByID[impact.ImpactID]
		if !supplied {
			continue
		}
		if impact.DecisionMode == "none" {
			return Plan{}, fmt.Errorf("%w: decision names non-review impact", ErrInvalidPlan)
		}
		if impact.DecisionMode == "consequence_only" && decision == "erase" {
			return Plan{}, fmt.Errorf("%w: consequence-only impact cannot be erased", ErrInvalidPlan)
		}
		if impact.DecisionMode == "content_candidate" && decision == "erase" {
			if impact.CandidateContentID == nil {
				return Plan{}, fmt.Errorf("%w: candidate missing", ErrInvalidPlan)
			}
			id, err := canonical.ParseID(*impact.CandidateContentID)
			if err != nil {
				return Plan{}, err
			}
			roots = append(roots, id)
			promoted[impact.ImpactID] = struct{}{}
		}
	}
	for id := range decisionByID {
		if _, exists := oldImpacts[id]; !exists {
			return Plan{}, fmt.Errorf("%w: decision names unknown impact", ErrInvalidPlan)
		}
	}
	slices.SortFunc(roots, func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
	roots = slices.Compact(roots)
	if plan.Scope == ScopeResident && len(promoted) != 0 {
		return Plan{}, fmt.Errorf("%w: resident plan cannot promote content", ErrInvalidPlan)
	}

	residentID, _ := canonical.ParseID(plan.ResidentID)
	actorID, _ := canonical.ParseID(plan.ActorPrincipalID)
	integrityPipelineID, _ := canonical.ParseID(plan.IntegrityPipelineVersionID)
	memoryStatusPipelineID, _ := canonical.ParseID(plan.MemoryStatusPipelineVersionID)
	request := PlanRequest{
		Scope:                         plan.Scope,
		ResidentID:                    residentID,
		ActorPrincipalID:              actorID,
		ReasonCode:                    plan.ReasonCode,
		IntegrityPipelineVersionID:    integrityPipelineID,
		MemoryStatusPipelineVersionID: memoryStatusPipelineID,
		RequestedContentIDs:           slices.Clone(roots),
	}
	captureRoots := slices.Clone(roots)
	if plan.Scope == ScopeResident {
		request.RequestedContentIDs = nil
		captureRoots = nil
	}
	snapshot, err := planner.Source.CaptureErasureSnapshot(ctx, CaptureRequest{
		Scope: plan.Scope, ResidentID: residentID, ActorPrincipalID: actorID,
		IntegrityPipelineVersionID: integrityPipelineID, MemoryStatusPipelineVersionID: memoryStatusPipelineID,
		RequestedContentIDs: captureRoots,
	})
	if err != nil {
		return Plan{}, err
	}
	if snapshot.HeadCommitID.String() != plan.BaseHead.CommitID || strconv.FormatInt(snapshot.HeadCommitSeq.Int64(), 10) != plan.BaseHead.CommitSeq {
		return Plan{}, ErrPlanStale
	}
	rebuilt, err := planner.build(ctx, request, request.RequestedContentIDs, snapshot, &plan)
	if err != nil {
		return Plan{}, err
	}
	for index := range rebuilt.Impacts {
		impact := &rebuilt.Impacts[index]
		if impact.DecisionMode == "none" {
			continue
		}
		old, existed := oldImpacts[impact.ImpactID]
		if !existed {
			impact.Decision = nil
			continue
		}
		if decision, supplied := decisionByID[impact.ImpactID]; supplied {
			value := decision
			impact.Decision = &value
		} else if old.Decision != nil {
			value := *old.Decision
			impact.Decision = &value
		}
	}
	for impactID := range promoted {
		for _, impact := range rebuilt.Impacts {
			if impact.ImpactID == impactID {
				return Plan{}, fmt.Errorf("%w: promoted candidate remains review-only", ErrInvalidPlan)
			}
		}
	}
	unresolved := false
	for _, impact := range rebuilt.Impacts {
		if impact.DecisionMode != "none" && impact.Decision == nil {
			unresolved = true
			break
		}
	}
	if unresolved || len(rebuilt.Blockers) != 0 {
		rebuilt.PlanState = StateReviewRequired
	} else {
		rebuilt.PlanState = StateReady
	}
	return Seal(rebuilt)
}

type targetWork struct {
	content ContentSnapshot
	depth   int
	parent  *canonical.ID
	event   canonical.ID
}

func (planner Planner) build(ctx context.Context, request PlanRequest, roots []canonical.ID, snapshot Snapshot, prior *Plan) (Plan, error) {
	plan := Plan{FormatVersion: PlanFormatVersion, RulesVersion: ImpactRulesVersion, Scope: request.Scope, ResidentID: request.ResidentID.String(), BaseHead: BaseHead{snapshot.HeadCommitID.String(), strconv.FormatInt(snapshot.HeadCommitSeq.Int64(), 10)}, ActorPrincipalID: request.ActorPrincipalID.String(), ReasonCode: request.ReasonCode, IntegrityPipelineVersionID: request.IntegrityPipelineVersionID.String(), MemoryStatusPipelineVersionID: request.MemoryStatusPipelineVersionID.String(), RequestedContentIDs: make([]string, len(roots)), EffectiveTargets: []EffectiveTarget{}, Impacts: []Impact{}, PlannedFindings: []PlannedFinding{}, ExistingFindingDependencies: []ExistingFindingDependency{}, PlannedQuarantines: []PlannedQuarantine{}, ClaimIdentityErasures: []ClaimIdentityErasure{}, ExistingClaimIdentityDependencies: []ExistingClaimIdentityDependency{}, RuntimeConfigEffect: RuntimeConfigEffect{}, Rebuilds: []Rebuild{}, Blockers: []Blocker{}, ExternalCopyNotice: ExternalCopyNotice}
	plan.Blockers = append(plan.Blockers, snapshot.Blockers...)
	for i, id := range roots {
		plan.RequestedContentIDs[i] = id.String()
	}
	if !snapshot.OwnerHuman {
		plan.Blockers = append(plan.Blockers, makeBlocker("actor_not_owner_human", request.ResidentID.String(), "owner_principal_id", nil))
	}
	contentByID := map[canonical.ID]ContentSnapshot{}
	for _, c := range snapshot.Contents {
		contentByID[c.ContentID] = c
	}
	if request.Scope == ScopeResident {
		for _, c := range snapshot.Contents {
			if c.ResidentID == request.ResidentID && c.ErasureState == "present" {
				roots = append(roots, c.ContentID)
			}
		}
		slices.SortFunc(roots, func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
	}
	work := map[canonical.ID]*targetWork{}
	queue := []canonical.ID{}
	for _, root := range roots {
		c, ok := contentByID[root]
		if !ok {
			plan.Blockers = append(plan.Blockers, makeBlocker("target_missing", root.String(), "existence", nil))
			continue
		}
		if c.ResidentID != request.ResidentID {
			plan.Blockers = append(plan.Blockers, makeBlocker("cross_resident_reference", root.String(), "cross_resident_reference", nil))
			continue
		}
		if c.ErasureState != "present" {
			plan.Blockers = append(plan.Blockers, makeBlocker("target_already_erased", root.String(), "erasure_state", nil))
			continue
		}
		if request.Scope == ScopeContent && c.ErasurePolicy != "independent" {
			plan.Blockers = append(plan.Blockers, makeBlocker("resident_only_in_content_scope", root.String(), "erasure_policy", nil))
			continue
		}
		work[root] = &targetWork{content: c}
		queue = append(queue, root)
	}
	lineage := slices.Clone(snapshot.Lineage)
	slices.SortFunc(lineage, func(a, b LineageEdge) int {
		if value := strings.Compare(a.ParentContentID.String(), b.ParentContentID.String()); value != 0 {
			return value
		}
		if value := strings.Compare(a.ChildContentID.String(), b.ChildContentID.String()); value != 0 {
			return value
		}
		return strings.Compare(a.RunID.String(), b.RunID.String())
	})
	if cycleID, found := reachableLineageCycle(roots, lineage, contentByID, request.ResidentID); found {
		plan.Blockers = append(plan.Blockers, makeBlocker("lineage_cycle", cycleID.String(), "cycle", nil))
	}
	// Fixed-point downstream closure. Invalid/cross-resident edges block; an
	// exact typed edge is must-erase, while a non-exact edge remains review.
	for cursor := 0; cursor < len(queue); cursor++ {
		parent := queue[cursor]
		pw := work[parent]
		for _, edge := range lineage {
			if edge.ParentContentID != parent {
				continue
			}
			child, ok := contentByID[edge.ChildContentID]
			if !ok || !edge.Valid {
				plan.Blockers = append(plan.Blockers, makeBlocker("unknown_lineage_source", edge.RunID.String(), "source", nil))
				continue
			}
			if child.ResidentID != request.ResidentID {
				plan.Blockers = append(plan.Blockers, makeBlocker("cross_resident_reference", child.ContentID.String(), "cross_resident_reference", nil))
				continue
			}
			if child.ErasureState != "present" {
				plan.Blockers = append(plan.Blockers, makeBlocker("target_already_erased", child.ContentID.String(), "erasure_state", nil))
				continue
			}
			if request.Scope == ScopeContent && child.ErasurePolicy != "independent" {
				plan.Blockers = append(plan.Blockers, makeBlocker("resident_only_in_content_scope", child.ContentID.String(), "erasure_policy", nil))
				continue
			}
			if existing, seen := work[child.ContentID]; seen {
				if edge.SameLogicalBlob {
					newDepth := pw.depth + 1
					if newDepth < existing.depth || (newDepth == existing.depth && existing.parent != nil && strings.Compare(parent.String(), existing.parent.String()) < 0) {
						shorter := newDepth < existing.depth
						parentCopy := parent
						existing.depth = newDepth
						existing.parent = &parentCopy
						if shorter {
							queue = append(queue, child.ContentID)
						}
					}
				}
				continue
			}
			if edge.SameLogicalBlob {
				parentCopy := parent
				work[child.ContentID] = &targetWork{content: child, depth: pw.depth + 1, parent: &parentCopy}
				queue = append(queue, child.ContentID)
			} else {
				candidate := child.ContentID.String()
				impact := Impact{RuleID: "explicit_lineage_nonexact_bytes_v1", ReferrerKind: "content_object", ReferrerID: child.ContentID.String(), ReferrerField: "derived_bytes", Classification: "needs_review", Actions: []string{}, DecisionMode: "content_candidate", CandidateContentID: &candidate}
				impact.ImpactID, _ = ImpactID(impact)
				plan.Impacts = append(plan.Impacts, impact)
			}
		}
	}
	ordered := make([]*targetWork, 0, len(work))
	// Equal bytes without a typed downstream edge are never auto-erased. Each
	// distinct candidate remains an explicit review decision.
	for _, candidate := range snapshot.Contents {
		if candidate.ResidentID != request.ResidentID || candidate.ErasureState != "present" || candidate.BlobHash == nil {
			continue
		}
		if _, alreadyTarget := work[candidate.ContentID]; alreadyTarget {
			continue
		}
		matched := false
		for _, target := range work {
			if target.content.BlobHash != nil && *target.content.BlobHash == *candidate.BlobHash {
				matched = true
				break
			}
		}
		if matched {
			candidateID := candidate.ContentID.String()
			impact := Impact{RuleID: "same_bytes_without_lineage_v1", ReferrerKind: "content_object", ReferrerID: candidateID,
				ReferrerField: "shared_bytes", Classification: "needs_review", Actions: []string{"block_physical_gc"},
				DecisionMode: "content_candidate", CandidateContentID: &candidateID}
			impact.ImpactID, _ = ImpactID(impact)
			plan.Impacts = append(plan.Impacts, impact)
		}
	}
	for _, w := range work {
		ordered = append(ordered, w)
	}
	slices.SortFunc(ordered, func(a, b *targetWork) int {
		if a.depth != b.depth {
			return a.depth - b.depth
		}
		if a.parent == nil && b.parent != nil {
			return -1
		}
		if a.parent != nil && b.parent == nil {
			return 1
		}
		if a.parent != nil {
			if c := strings.Compare(a.parent.String(), b.parent.String()); c != 0 {
				return c
			}
		}
		return strings.Compare(a.content.ContentID.String(), b.content.ContentID.String())
	})
	priorTargets := map[canonical.ID]canonical.ID{}
	if prior != nil {
		for _, target := range prior.EffectiveTargets {
			contentID, _ := canonical.ParseID(target.ContentID)
			eventID, _ := canonical.ParseID(target.ErasureEventID)
			priorTargets[contentID] = eventID
		}
	}
	for _, w := range ordered {
		if existing, ok := priorTargets[w.content.ContentID]; ok {
			w.event = existing
			continue
		}
		id, err := planner.IDs.New()
		if err != nil {
			return Plan{}, err
		}
		w.event = id
	}
	for _, w := range ordered {
		var source *string
		if w.parent != nil {
			s := work[*w.parent].event.String()
			source = &s
		}
		plan.EffectiveTargets = append(plan.EffectiveTargets, EffectiveTarget{w.content.ContentID.String(), w.content.ContentClass, w.content.ErasurePolicy, "sha256:" + w.content.Commitment.Hex(), w.event.String(), source, strconv.Itoa(w.depth)})
		if w.depth > 0 {
			impact := Impact{RuleID: "explicit_lineage_exact_bytes_v1", ReferrerKind: "content_object", ReferrerID: w.content.ContentID.String(), ReferrerField: "derived_bytes", Classification: "must_erase", Actions: []string{"erase_derived_content"}, DecisionMode: "none"}
			impact.ImpactID, _ = ImpactID(impact)
			plan.Impacts = append(plan.Impacts, impact)
		}
		appendBlobBlocker := func(status, field string, valid bool) {
			if status == "valid" || (status == "" && valid) {
				return
			}
			code := "blob_missing"
			if status == "hash_mismatch" {
				code = "blob_hash_mismatch"
			}
			plan.Blockers = append(plan.Blockers, makeBlocker(code, w.content.ContentID.String(), field, nil))
		}
		if w.content.BlobHash == nil {
			plan.Blockers = append(plan.Blockers, makeBlocker("blob_missing", w.content.ContentID.String(), "sqlite_blob", nil), makeBlocker("blob_missing", w.content.ContentID.String(), "filesystem_blob", nil))
		} else {
			appendBlobBlocker(w.content.SQLiteBlobStatus, "sqlite_blob", w.content.SQLiteBlobValid)
			appendBlobBlocker(w.content.FilesystemBlobStatus, "filesystem_blob", w.content.FilesystemBlobValid)
		}
	}
	targets := map[canonical.ID]*targetWork{}
	for _, w := range ordered {
		targets[w.content.ContentID] = w
	}
	for _, ref := range snapshot.DirectReferences {
		w, ok := targets[ref.ContentID]
		if !ok {
			continue
		}
		descriptor, ok := impactCatalog()[ref.RuleID]
		if !ok {
			continue
		}
		impact := Impact{RuleID: ref.RuleID, ReferrerKind: ref.ReferrerKind, ReferrerID: ref.ReferrerID, ReferrerField: ref.ReferrerField, Classification: descriptor.Classification, Actions: slices.Clone(descriptor.Actions), DecisionMode: descriptor.DecisionMode}
		impact.ImpactID, _ = ImpactID(impact)
		plan.Impacts = append(plan.Impacts, impact)
		_ = w
	}
	for _, alias := range snapshot.Aliases {
		w, ok := targets[alias.StatementContentID]
		if !ok {
			if request.Scope != ScopeResident {
				continue
			}
			content, exists := contentByID[alias.StatementContentID]
			if !exists || content.ResidentID != request.ResidentID || alias.ResidentID != request.ResidentID {
				plan.Blockers = append(plan.Blockers, makeBlocker("cross_resident_reference", alias.StatementContentID.String(), "cross_resident_reference", nil))
				continue
			}
			if content.ErasureState == "erased" && !alias.StatementHashPresent && alias.ClaimStatementErasureEventID != nil && alias.ContentErasureEventID != nil && alias.CanonicalCommitID != nil {
				plan.ExistingClaimIdentityDependencies = append(plan.ExistingClaimIdentityDependencies, ExistingClaimIdentityDependency{alias.ClaimStatementErasureEventID.String(), alias.ClaimID.String(), alias.StatementContentID.String(), alias.ContentErasureEventID.String(), alias.CanonicalCommitID.String()})
			} else {
				plan.Blockers = append(plan.Blockers, makeBlocker("planned_effect_mismatch", "", "planned_effects", nil))
			}
			continue
		}
		if alias.ResidentID != request.ResidentID {
			plan.Blockers = append(plan.Blockers, makeBlocker("cross_resident_reference", alias.StatementContentID.String(), "cross_resident_reference", nil))
			continue
		}
		if alias.StatementHashPresent && alias.ClaimStatementErasureEventID == nil {
			pair, reused := priorClaimErasureID(prior, alias.ClaimID, alias.StatementContentID)
			var err error
			if !reused {
				pair, err = planner.IDs.New()
			}
			if err != nil {
				return Plan{}, err
			}
			plan.ClaimIdentityErasures = append(plan.ClaimIdentityErasures, ClaimIdentityErasure{pair.String(), alias.ClaimID.String(), alias.StatementContentID.String(), w.event.String()})
			findingID, reusedFinding := priorFindingID(prior, alias.ClaimID)
			if !reusedFinding {
				findingID, err = planner.IDs.New()
			}
			if err != nil {
				return Plan{}, err
			}
			claimID := alias.ClaimID
			eventID := w.event
			candidate, err := integrity.NewCandidate(integrity.CandidateInput{ResidentID: request.ResidentID, ClaimID: &claimID, Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimStatementErased, TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "statement_content_id", SourceContentErasureEventID: &eventID, OccurredTZ: canonical.Timezone("UTC")})
			if err != nil {
				return Plan{}, err
			}
			fingerprint := "sha256:" + candidate.Fingerprint.Hex()
			source := eventID.String()
			claim := claimID.String()
			plan.PlannedFindings = append(plan.PlannedFindings, PlannedFinding{findingID.String(), string(candidate.Kind), string(candidate.RuleCode), string(candidate.TargetKind), claim, candidate.TargetField, request.ResidentID.String(), &claim, &source, request.IntegrityPipelineVersionID.String(), fingerprint, nil})
			provenanceActions := []string{"record_integrity_finding"}
			if alias.CurrentStatus == "active" {
				transition, reusedTransition := priorQuarantineID(prior, alias.ClaimID)
				if !reusedTransition {
					transition, err = planner.IDs.New()
				}
				if err != nil {
					return Plan{}, err
				}
				plan.PlannedQuarantines = append(plan.PlannedQuarantines, PlannedQuarantine{StatusTransitionID: transition.String(), ClaimID: claim, FromStatus: "active", ToStatus: "quarantined", DecisionKind: "automatic", TriggerKind: "integrity_finding", TriggerIntegrityFindingID: findingID.String(), PipelineVersionID: request.MemoryStatusPipelineVersionID.String(), GateMetrics: `{"reason":"required_provenance_erased"}`, DecisionReasonCode: "structural_quarantine"})
				provenanceActions = append(provenanceActions, "quarantine_claim")
			}
			slices.Sort(provenanceActions)
			provenance := Impact{RuleID: "claim_provenance_break_v1", ReferrerKind: "claim", ReferrerID: claim, ReferrerField: "qualifying_provenance", Classification: "needs_review", Actions: provenanceActions, DecisionMode: "consequence_only"}
			provenance.ImpactID, _ = ImpactID(provenance)
			plan.Impacts = append(plan.Impacts, provenance)
		} else {
			plan.Blockers = append(plan.Blockers, makeBlocker("planned_effect_mismatch", "", "planned_effects", nil))
		}
	}
	if logicalSource, ok := planner.Source.(LogicalProvenanceSource); ok && len(ordered) != 0 {
		logicalRequest := LogicalProvenanceRequest{
			ResidentID: request.ResidentID, BaseHeadCommitID: snapshot.HeadCommitID,
			BaseHeadCommitSeq: snapshot.HeadCommitSeq,
			Targets:           make([]LogicalErasureTarget, 0, len(ordered)),
		}
		for _, w := range ordered {
			logicalRequest.Targets = append(logicalRequest.Targets, LogicalErasureTarget{ContentID: w.content.ContentID, ErasureEventID: w.event})
		}
		logical, err := logicalSource.CaptureErasureLogicalProvenance(ctx, logicalRequest)
		if err != nil {
			return Plan{}, err
		}
		plan.Blockers = append(plan.Blockers, logical.Blockers...)
		for _, reference := range logical.References {
			impact, err := ImpactForLogicalReference(reference)
			if err != nil {
				return Plan{}, err
			}
			plan.Impacts = append(plan.Impacts, impact)
		}
		for _, dependency := range logical.ExistingFindingDependencies {
			finding := dependency.Finding
			var source *string
			if finding.SourceContentErasureEventID != nil {
				value := finding.SourceContentErasureEventID.String()
				source = &value
			}
			plan.ExistingFindingDependencies = append(plan.ExistingFindingDependencies, ExistingFindingDependency{
				IntegrityFindingID: finding.IntegrityFindingID.String(), ClaimID: dependency.ClaimID.String(), FindingKind: finding.FindingKind,
				RuleCode: finding.RuleCode, SourceContentErasureEventID: source, PipelineVersionID: finding.PipelineVersionID.String(),
				FindingFingerprint: "sha256:" + finding.Fingerprint.Hex(),
			})
			if dependency.CurrentStatus == "active" {
				if err := planner.appendProvenanceQuarantine(&plan, request, prior, dependency.ClaimID, finding.IntegrityFindingID); err != nil {
					return Plan{}, err
				}
				appendClaimProvenanceImpact(&plan, dependency.ClaimID.String(), []string{"quarantine_claim"})
			}
		}
		for _, broken := range logical.Breaks {
			if err := planner.appendSupportProvenanceBreak(&plan, request, prior, broken); err != nil {
				return Plan{}, err
			}
		}
	}
	if request.Scope == ScopeResident {
		var transition canonical.ID
		var err error
		if prior != nil && prior.ResidentTransition != nil {
			transition, _ = canonical.ParseID(prior.ResidentTransition.TransitionID)
		} else {
			transition, err = planner.IDs.New()
		}
		if err != nil {
			return Plan{}, err
		}
		plan.ResidentTransition = &ResidentTransition{TransitionID: transition.String(), ResidentID: request.ResidentID.String(), FromStatus: snapshot.ResidentStatus, ToStatus: "erased", ActorPrincipalID: request.ActorPrincipalID.String(), ReasonCode: request.ReasonCode}
		if snapshot.ActiveResidentID != nil && *snapshot.ActiveResidentID == request.ResidentID {
			expected := request.ResidentID.String()
			plan.RuntimeConfigEffect = RuntimeConfigEffect{ExpectedActiveResidentID: &expected, ClearActiveResident: true}
		}
		if snapshot.MandatoryWorkCount > 0 {
			actions := []string{"replan_erasure", "run_recovery_terminalize"}
			if snapshot.ResidentStatus == "active" {
				actions = []string{"archive_resident", "replan_erasure", "run_recovery_terminalize"}
			}
			plan.Blockers = append(plan.Blockers, makeBlocker("mandatory_work_present", request.ResidentID.String(), "mandatory_work", actions))
		}
		lifecycleActions := []string{"rebuild_projection", "transition_resident_erased"}
		if plan.RuntimeConfigEffect.ClearActiveResident {
			lifecycleActions = []string{"clear_runtime_selection", "rebuild_projection", "transition_resident_erased"}
		}
		lifecycle := Impact{RuleID: "resident_lifecycle_erase_v1", ReferrerKind: "resident", ReferrerID: request.ResidentID.String(), ReferrerField: "lifecycle", Classification: "needs_rebuild", Actions: lifecycleActions, DecisionMode: "none"}
		lifecycle.ImpactID, _ = ImpactID(lifecycle)
		plan.Impacts = append(plan.Impacts, lifecycle)
	}
	for _, running := range snapshot.Running {
		if request.Scope == ScopeResident || runningIntersectsTargets(running, snapshot.Lineage, targets) {
			plan.Blockers = append(plan.Blockers, makeBlocker("running_attempt", running.RunID.String(), "attempt", nil))
		}
	}
	for _, definition := range snapshot.ProjectionDefinitions {
		projectionImpact := Impact{RuleID: "projection_derived_state_v1", ReferrerKind: "projection", ReferrerID: definition.Name, ReferrerField: "body", Classification: "needs_rebuild", Actions: []string{"rebuild_projection"}, DecisionMode: "none"}
		projectionImpact.ImpactID, _ = ImpactID(projectionImpact)
		plan.Impacts = append(plan.Impacts, projectionImpact)
		plan.Rebuilds = append(plan.Rebuilds, Rebuild{request.ResidentID.String(), definition.Name, definition.Version, "content_erasure"})
		if len(plan.PlannedQuarantines) != 0 {
			plan.Rebuilds = append(plan.Rebuilds, Rebuild{request.ResidentID.String(), definition.Name, definition.Version, "claim_state_change"})
		}
		if request.Scope == ScopeResident {
			plan.Rebuilds = append(plan.Rebuilds, Rebuild{request.ResidentID.String(), definition.Name, definition.Version, "resident_lifecycle_change"})
		}
	}
	if plannedMutationCount(plan) > MaxSingleTransactionMutations {
		plan.Blockers = append(plan.Blockers, makeBlocker("transaction_size_unsupported", request.ResidentID.String(), "effective_targets", nil))
	}
	sortPlanCollections(&plan)
	unresolved := false
	for _, impact := range plan.Impacts {
		if impact.DecisionMode != "none" {
			unresolved = true
			break
		}
	}
	if unresolved || len(plan.Blockers) > 0 {
		plan.PlanState = StateReviewRequired
	} else {
		plan.PlanState = StateReady
	}
	return Seal(plan)
}

func runningIntersectsTargets(running RunningWork, lineage []LineageEdge, targets map[canonical.ID]*targetWork) bool {
	for _, contentID := range running.ContentIDs {
		if _, ok := targets[contentID]; ok {
			return true
		}
	}
	for _, edge := range lineage {
		if edge.RunID != running.RunID {
			continue
		}
		if _, ok := targets[edge.ParentContentID]; ok {
			return true
		}
		if _, ok := targets[edge.ChildContentID]; ok {
			return true
		}
	}
	return false
}

func makeBlocker(code, id, field string, actions []string) Blocker {
	contract := blockerCatalog[code]
	var targetID *string
	if !contract.Global {
		targetID = &id
	}
	targetField := field
	if actions == nil {
		actions = slices.Clone(contract.Actions)
	}
	slices.Sort(actions)
	return Blocker{code, contract.Kind, targetID, &targetField, actions}
}
func reachableLineageCycle(roots []canonical.ID, edges []LineageEdge, contents map[canonical.ID]ContentSnapshot, residentID canonical.ID) (canonical.ID, bool) {
	adjacency := map[canonical.ID][]canonical.ID{}
	for _, edge := range edges {
		parent, parentOK := contents[edge.ParentContentID]
		child, childOK := contents[edge.ChildContentID]
		if !edge.Valid || !parentOK || !childOK || parent.ResidentID != residentID || child.ResidentID != residentID || parent.ErasureState != "present" || child.ErasureState != "present" {
			continue
		}
		adjacency[edge.ParentContentID] = append(adjacency[edge.ParentContentID], edge.ChildContentID)
	}
	for parent := range adjacency {
		slices.SortFunc(adjacency[parent], func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
		adjacency[parent] = slices.Compact(adjacency[parent])
	}
	state := map[canonical.ID]uint8{}
	stack := []canonical.ID{}
	stackIndex := map[canonical.ID]int{}
	var minimum canonical.ID
	var visit func(canonical.ID)
	visit = func(node canonical.ID) {
		state[node] = 1
		stackIndex[node] = len(stack)
		stack = append(stack, node)
		for _, child := range adjacency[node] {
			switch state[child] {
			case 0:
				visit(child)
			case 1:
				candidate := child
				for _, cycleNode := range stack[stackIndex[child]:] {
					if strings.Compare(cycleNode.String(), candidate.String()) < 0 {
						candidate = cycleNode
					}
				}
				if minimum.IsZero() || strings.Compare(candidate.String(), minimum.String()) < 0 {
					minimum = candidate
				}
			}
		}
		stack = stack[:len(stack)-1]
		delete(stackIndex, node)
		state[node] = 2
	}
	for _, root := range roots {
		if state[root] == 0 {
			visit(root)
		}
	}
	return minimum, !minimum.IsZero()
}

func priorClaimErasureID(prior *Plan, claimID, contentID canonical.ID) (canonical.ID, bool) {
	if prior == nil {
		return canonical.ID{}, false
	}
	for _, value := range prior.ClaimIdentityErasures {
		if value.ClaimID == claimID.String() && value.StatementContentID == contentID.String() {
			id, err := canonical.ParseID(value.ClaimStatementErasureEventID)
			return id, err == nil
		}
	}
	return canonical.ID{}, false
}

func priorFindingID(prior *Plan, claimID canonical.ID) (canonical.ID, bool) {
	if prior == nil {
		return canonical.ID{}, false
	}
	for _, value := range prior.PlannedFindings {
		if value.ClaimID != nil && *value.ClaimID == claimID.String() && value.RuleCode == string(integrity.RuleClaimStatementErased) {
			id, err := canonical.ParseID(value.IntegrityFindingID)
			return id, err == nil
		}
	}
	return canonical.ID{}, false
}

func priorFindingIDForRule(prior *Plan, claimID canonical.ID, rule integrity.RuleCode) (canonical.ID, bool) {
	if prior == nil {
		return canonical.ID{}, false
	}
	for _, value := range prior.PlannedFindings {
		if value.ClaimID != nil && *value.ClaimID == claimID.String() && value.RuleCode == string(rule) {
			id, err := canonical.ParseID(value.IntegrityFindingID)
			return id, err == nil
		}
	}
	return canonical.ID{}, false
}

func priorQuarantineID(prior *Plan, claimID canonical.ID) (canonical.ID, bool) {
	if prior == nil {
		return canonical.ID{}, false
	}
	for _, value := range prior.PlannedQuarantines {
		if value.ClaimID == claimID.String() {
			id, err := canonical.ParseID(value.StatusTransitionID)
			return id, err == nil
		}
	}
	return canonical.ID{}, false
}

func (planner Planner) appendSupportProvenanceBreak(plan *Plan, request PlanRequest, prior *Plan, broken ProvenanceBreakSnapshot) error {
	findingID, reused := priorFindingIDForRule(prior, broken.ClaimID, integrity.RuleClaimQualifyingSupportErased)
	var err error
	if !reused {
		findingID, err = planner.IDs.New()
	}
	if err != nil {
		return err
	}
	claimID := broken.ClaimID
	sourceID := broken.SourceContentErasureEventID
	candidate, err := integrity.NewCandidate(integrity.CandidateInput{
		ResidentID: request.ResidentID, ClaimID: &claimID,
		Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimQualifyingSupportErased,
		TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "qualifying_support",
		SourceContentErasureEventID: &sourceID, OccurredTZ: canonical.Timezone("UTC"),
	})
	if err != nil {
		return err
	}
	claim, source := claimID.String(), sourceID.String()
	plan.PlannedFindings = append(plan.PlannedFindings, PlannedFinding{
		IntegrityFindingID: findingID.String(), FindingKind: string(candidate.Kind), RuleCode: string(candidate.RuleCode),
		TargetKind: string(candidate.TargetKind), TargetID: claim, TargetField: candidate.TargetField,
		ResidentID: request.ResidentID.String(), ClaimID: &claim, SourceContentErasureEventID: &source,
		PipelineVersionID: request.IntegrityPipelineVersionID.String(), FindingFingerprint: "sha256:" + candidate.Fingerprint.Hex(),
	})
	actions := []string{"record_integrity_finding"}
	if broken.CurrentStatus == "active" {
		if err := planner.appendProvenanceQuarantine(plan, request, prior, claimID, findingID); err != nil {
			return err
		}
		actions = append(actions, "quarantine_claim")
	}
	appendClaimProvenanceImpact(plan, claim, actions)
	return nil
}

func (planner Planner) appendProvenanceQuarantine(plan *Plan, request PlanRequest, prior *Plan, claimID, findingID canonical.ID) error {
	transition, reused := priorQuarantineID(prior, claimID)
	var err error
	if !reused {
		transition, err = planner.IDs.New()
	}
	if err != nil {
		return err
	}
	plan.PlannedQuarantines = append(plan.PlannedQuarantines, PlannedQuarantine{
		StatusTransitionID: transition.String(), ClaimID: claimID.String(), FromStatus: "active", ToStatus: "quarantined",
		DecisionKind: "automatic", TriggerKind: "integrity_finding", TriggerIntegrityFindingID: findingID.String(),
		PipelineVersionID: request.MemoryStatusPipelineVersionID.String(), GateMetrics: `{"reason":"required_provenance_erased"}`,
		DecisionReasonCode: "structural_quarantine",
	})
	return nil
}

func appendClaimProvenanceImpact(plan *Plan, claimID string, actions []string) {
	slices.Sort(actions)
	impact := Impact{RuleID: "claim_provenance_break_v1", ReferrerKind: "claim", ReferrerID: claimID,
		ReferrerField: "qualifying_provenance", Classification: "needs_review", Actions: actions, DecisionMode: "consequence_only"}
	impact.ImpactID, _ = ImpactID(impact)
	plan.Impacts = append(plan.Impacts, impact)
}

func sortPlanCollections(p *Plan) {
	slices.SortFunc(p.EffectiveTargets, compareTarget)
	slices.SortFunc(p.Impacts, func(a, b Impact) int { return strings.Compare(a.ImpactID, b.ImpactID) })
	p.Impacts = slices.CompactFunc(p.Impacts, func(a, b Impact) bool { return a.ImpactID == b.ImpactID })
	slices.SortFunc(p.PlannedFindings, func(a, b PlannedFinding) int {
		return strings.Compare(a.FindingFingerprint+"\x00"+a.IntegrityFindingID, b.FindingFingerprint+"\x00"+b.IntegrityFindingID)
	})
	slices.SortFunc(p.ExistingFindingDependencies, func(a, b ExistingFindingDependency) int {
		return strings.Compare(a.FindingFingerprint+"\x00"+a.IntegrityFindingID, b.FindingFingerprint+"\x00"+b.IntegrityFindingID)
	})
	slices.SortFunc(p.PlannedQuarantines, func(a, b PlannedQuarantine) int {
		return strings.Compare(a.ClaimID+"\x00"+a.StatusTransitionID, b.ClaimID+"\x00"+b.StatusTransitionID)
	})
	slices.SortFunc(p.ClaimIdentityErasures, func(a, b ClaimIdentityErasure) int {
		return strings.Compare(a.ClaimID+"\x00"+a.ClaimStatementErasureEventID, b.ClaimID+"\x00"+b.ClaimStatementErasureEventID)
	})
	slices.SortFunc(p.ExistingClaimIdentityDependencies, func(a, b ExistingClaimIdentityDependency) int {
		return strings.Compare(a.ClaimID+"\x00"+a.ClaimStatementErasureEventID, b.ClaimID+"\x00"+b.ClaimStatementErasureEventID)
	})
	slices.SortFunc(p.Rebuilds, func(a, b Rebuild) int {
		return strings.Compare(a.ResidentID+"\x00"+a.ProjectionName+"\x00"+a.ProjectionVersion+"\x00"+a.ReasonCode, b.ResidentID+"\x00"+b.ProjectionName+"\x00"+b.ProjectionVersion+"\x00"+b.ReasonCode)
	})
	slices.SortFunc(p.Blockers, func(a, b Blocker) int {
		key := func(v Blocker) string {
			s := v.Code + "\x00" + v.TargetKind + "\x00"
			if v.TargetID != nil {
				s += *v.TargetID
			}
			s += "\x00"
			if v.TargetField != nil {
				s += *v.TargetField
			}
			return s
		}
		return strings.Compare(key(a), key(b))
	})
	p.Blockers = slices.CompactFunc(p.Blockers, func(a, b Blocker) bool {
		return a.Code == b.Code && a.TargetKind == b.TargetKind && compareNullable(a.TargetID, b.TargetID) == 0 && compareNullable(a.TargetField, b.TargetField) == 0
	})
}
