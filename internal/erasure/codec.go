package erasure

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

var (
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	reasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// Seal validates the complete closed plan and derives its domain-separated
// digest. It never mutates collection ordering on the caller's behalf.
func Seal(plan Plan) (Plan, error) {
	plan.FormatVersion = PlanFormatVersion
	plan.RulesVersion = ImpactRulesVersion
	plan.ExternalCopyNotice = ExternalCopyNotice
	plan.Digest = "sha256:" + strings.Repeat("0", 64)
	if err := validatePlan(plan, false); err != nil {
		return Plan{}, err
	}
	digest, err := computePlanDigest(plan)
	if err != nil {
		return Plan{}, err
	}
	plan.Digest = digest
	if err := validatePlan(plan, true); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func MarshalPlan(plan Plan) ([]byte, error) {
	if err := validatePlan(plan, true); err != nil {
		return nil, err
	}
	encoded, err := canonical.MarshalCanonical(plan)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %v", ErrInvalidPlan, err)
	}
	return append(encoded.Bytes(), '\n'), nil
}

func ParsePlan(input []byte) (Plan, error) {
	var plan Plan
	if len(input) < 2 || input[len(input)-1] != '\n' || input[len(input)-2] == '\n' || bytes.Contains(input[:len(input)-1], []byte{'\r'}) {
		return plan, fmt.Errorf("%w: plan must be one JCS object plus LF", ErrInvalidPlan)
	}
	body := input[:len(input)-1]
	if _, err := canonical.ParseCanonicalJSON(body); err != nil {
		return plan, fmt.Errorf("%w: plan is not exact JCS: %v", ErrInvalidPlan, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("%w: decode: %v", ErrInvalidPlan, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Plan{}, fmt.Errorf("%w: trailing data", ErrInvalidPlan)
	}
	if err := validatePlan(plan, true); err != nil {
		return Plan{}, err
	}
	reencoded, err := MarshalPlan(plan)
	if err != nil || !bytes.Equal(reencoded, input) {
		return Plan{}, fmt.Errorf("%w: bytes differ from exact schema encoding", ErrInvalidPlan)
	}
	return plan, nil
}

func PlanDigest(plan Plan) (string, error) {
	if err := validatePlan(plan, true); err != nil {
		return "", err
	}
	return computePlanDigest(plan)
}

func computePlanDigest(plan Plan) (string, error) {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return "", err
	}
	delete(object, "digest")
	canonicalBody, err := canonical.MarshalCanonical(object)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("mahoroba:erasure-plan-digest:v1"))
	hash.Write([]byte{0})
	hash.Write(canonicalBody.Bytes())
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func ImpactID(value Impact) (string, error) {
	body, err := canonical.MarshalCanonical(struct {
		RuleID             string  `json:"rule_id"`
		ReferrerKind       string  `json:"referrer_kind"`
		ReferrerID         string  `json:"referrer_id"`
		ReferrerField      string  `json:"referrer_field"`
		DecisionMode       string  `json:"decision_mode"`
		CandidateContentID *string `json:"candidate_content_id"`
	}{value.RuleID, value.ReferrerKind, value.ReferrerID, value.ReferrerField, value.DecisionMode, value.CandidateContentID})
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("mahoroba:erasure-impact-id:v1"))
	hash.Write([]byte{0})
	hash.Write(body.Bytes())
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// Decide returns a new sealed plan. Decisions are complete and exact for the
// current review round; clients cannot remove must-erase targets or decide a
// consequence-only impact as erase.
func Decide(plan Plan, decisions []DecisionInput) (Plan, error) {
	if err := validatePlan(plan, true); err != nil {
		return Plan{}, err
	}
	if len(plan.Blockers) != 0 {
		return Plan{}, ErrBlocked
	}
	byID := make(map[string]string, len(decisions))
	for _, decision := range decisions {
		if decision.Decision != "retain" && decision.Decision != "erase" {
			return Plan{}, fmt.Errorf("%w: unknown decision", ErrInvalidPlan)
		}
		if _, exists := byID[decision.ImpactID]; exists {
			return Plan{}, fmt.Errorf("%w: duplicate decision", ErrInvalidPlan)
		}
		byID[decision.ImpactID] = decision.Decision
	}
	for index := range plan.Impacts {
		impact := &plan.Impacts[index]
		if impact.DecisionMode == "none" {
			continue
		}
		decision, ok := byID[impact.ImpactID]
		if !ok {
			impact.Decision = nil
			continue
		}
		if impact.DecisionMode == "consequence_only" && decision == "erase" {
			return Plan{}, fmt.Errorf("%w: consequence-only impact cannot be erased", ErrInvalidPlan)
		}
		value := decision
		impact.Decision = &value
		delete(byID, impact.ImpactID)
	}
	if len(byID) != 0 {
		return Plan{}, fmt.Errorf("%w: decision names non-review impact", ErrInvalidPlan)
	}
	for _, impact := range plan.Impacts {
		if impact.DecisionMode != "none" && impact.Decision == nil {
			plan.PlanState = StateReviewRequired
			return Seal(plan)
		}
		if impact.DecisionMode == "content_candidate" && impact.Decision != nil && *impact.Decision == "erase" {
			return Plan{}, fmt.Errorf("%w: erase decision requires closure replanning", ErrReviewRequired)
		}
	}
	plan.PlanState = StateReady
	return Seal(plan)
}

func validatePlan(plan Plan, verifyDigest bool) error {
	if plan.FormatVersion != PlanFormatVersion || plan.RulesVersion != ImpactRulesVersion || plan.ExternalCopyNotice != ExternalCopyNotice {
		return fmt.Errorf("%w: version/notice differs", ErrInvalidPlan)
	}
	if plan.Scope != ScopeContent && plan.Scope != ScopeResident {
		return fmt.Errorf("%w: invalid scope", ErrInvalidPlan)
	}
	if plan.PlanState != StateReady && plan.PlanState != StateReviewRequired {
		return fmt.Errorf("%w: invalid state", ErrInvalidPlan)
	}
	for _, raw := range []string{plan.ResidentID, plan.BaseHead.CommitID, plan.ActorPrincipalID, plan.IntegrityPipelineVersionID, plan.MemoryStatusPipelineVersionID} {
		if err := validateID(raw); err != nil {
			return err
		}
	}
	if !positiveDecimal(plan.BaseHead.CommitSeq) || !reasonPattern.MatchString(plan.ReasonCode) {
		return fmt.Errorf("%w: invalid head/reason", ErrInvalidPlan)
	}
	if nilCollections(plan) {
		return fmt.Errorf("%w: collections must not be null", ErrInvalidPlan)
	}
	if plan.Scope == ScopeContent && len(plan.RequestedContentIDs) == 0 {
		return fmt.Errorf("%w: content scope has no root", ErrInvalidPlan)
	}
	if !strictSortedStrings(plan.RequestedContentIDs) {
		return fmt.Errorf("%w: requested IDs unordered", ErrInvalidPlan)
	}
	for _, id := range plan.RequestedContentIDs {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if err := validateTargets(plan); err != nil {
		return err
	}
	if err := validateImpacts(plan); err != nil {
		return err
	}
	if err := validateEffects(plan); err != nil {
		return err
	}
	if err := validateScopeEffects(plan); err != nil {
		return err
	}
	if err := validatePlanRelationships(plan); err != nil {
		return err
	}
	if err := validateTransactionSizeContract(plan); err != nil {
		return err
	}
	unresolved := false
	for _, impact := range plan.Impacts {
		if impact.DecisionMode != "none" && impact.Decision == nil {
			unresolved = true
		}
	}
	if plan.PlanState == StateReady && (unresolved || len(plan.Blockers) != 0) {
		return fmt.Errorf("%w: ready plan is unresolved/blocked", ErrInvalidPlan)
	}
	if plan.PlanState == StateReviewRequired && !unresolved && len(plan.Blockers) == 0 {
		return fmt.Errorf("%w: review state has no unresolved review", ErrInvalidPlan)
	}
	if verifyDigest {
		if !digestPattern.MatchString(plan.Digest) {
			return fmt.Errorf("%w: invalid digest", ErrInvalidPlan)
		}
		want, err := computePlanDigest(plan)
		if err != nil {
			return err
		}
		if want != plan.Digest {
			return ErrPlanDigestMismatch
		}
	}
	return nil
}

func plannedMutationCount(plan Plan) int {
	count := 2*len(plan.EffectiveTargets) + len(plan.PlannedFindings) + len(plan.PlannedQuarantines) + 2*len(plan.ClaimIdentityErasures)
	if plan.ResidentTransition != nil {
		count++
	}
	if plan.RuntimeConfigEffect.ClearActiveResident {
		count++
	}
	return count
}

func validateTransactionSizeContract(plan Plan) error {
	overLimit := plannedMutationCount(plan) > MaxSingleTransactionMutations
	hasBlocker := false
	for _, blocker := range plan.Blockers {
		if blocker.Code == "transaction_size_unsupported" {
			hasBlocker = true
			if blocker.TargetID == nil || *blocker.TargetID != plan.ResidentID {
				return fmt.Errorf("%w: transaction-size blocker resident differs", ErrInvalidPlan)
			}
		}
	}
	if overLimit != hasBlocker {
		return fmt.Errorf("%w: transaction-size blocker does not match mutation bound", ErrInvalidPlan)
	}
	return nil
}

func validateTargets(plan Plan) error {
	previous := EffectiveTarget{}
	events := make(map[string]string, len(plan.EffectiveTargets))
	contents := make(map[string]struct{}, len(plan.EffectiveTargets))
	for index, value := range plan.EffectiveTargets {
		for _, id := range []string{value.ContentID, value.ErasureEventID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		if value.SourceErasureEventID != nil {
			if err := validateID(*value.SourceErasureEventID); err != nil {
				return err
			}
		}
		if value.ContentClass == "" || (value.ErasurePolicy != "independent" && value.ErasurePolicy != "resident_only") || !digestPattern.MatchString(value.Commitment) || !nonNegativeDecimal(value.LineageDepth) {
			return fmt.Errorf("%w: invalid target", ErrInvalidPlan)
		}
		if _, ok := contents[value.ContentID]; ok {
			return fmt.Errorf("%w: duplicate target", ErrInvalidPlan)
		}
		contents[value.ContentID] = struct{}{}
		if _, ok := events[value.ErasureEventID]; ok {
			return fmt.Errorf("%w: duplicate event", ErrInvalidPlan)
		}
		events[value.ErasureEventID] = value.ContentID
		if index > 0 && compareTarget(previous, value) >= 0 {
			return fmt.Errorf("%w: targets unordered", ErrInvalidPlan)
		}
		previous = value
	}
	for _, value := range plan.EffectiveTargets {
		depth, _ := strconv.Atoi(value.LineageDepth)
		if depth == 0 && value.SourceErasureEventID != nil {
			return fmt.Errorf("%w: root has source", ErrInvalidPlan)
		}
		if depth > 0 {
			if value.SourceErasureEventID == nil {
				return fmt.Errorf("%w: derived target lacks source", ErrInvalidPlan)
			}
			if _, ok := events[*value.SourceErasureEventID]; !ok {
				return fmt.Errorf("%w: derived source outside plan", ErrInvalidPlan)
			}
		}
		if plan.Scope == ScopeContent && value.ErasurePolicy != "independent" {
			return fmt.Errorf("%w: resident-only target in content scope", ErrInvalidPlan)
		}
	}
	return nil
}

func validateImpacts(plan Plan) error {
	previous := ""
	for _, value := range plan.Impacts {
		if !digestPattern.MatchString(value.ImpactID) || value.ReferrerID == "" || !strictSortedStrings(value.Actions) {
			return fmt.Errorf("%w: invalid impact", ErrInvalidPlan)
		}
		want, err := ImpactID(value)
		if err != nil || want != value.ImpactID {
			return fmt.Errorf("%w: impact ID mismatch", ErrInvalidPlan)
		}
		if previous != "" && value.ImpactID <= previous {
			return fmt.Errorf("%w: impacts unordered", ErrInvalidPlan)
		}
		previous = value.ImpactID
		if err := validateImpactAgainstCatalog(value); err != nil {
			return err
		}
		switch value.DecisionMode {
		case "none":
			if value.CandidateContentID != nil || value.Decision != nil {
				return fmt.Errorf("%w: non-review impact has decision", ErrInvalidPlan)
			}
		case "content_candidate":
			if value.CandidateContentID == nil {
				return fmt.Errorf("%w: content candidate missing", ErrInvalidPlan)
			}
			if err := validateID(*value.CandidateContentID); err != nil {
				return err
			}
			fallthrough
		case "consequence_only":
			if value.DecisionMode == "consequence_only" && value.CandidateContentID != nil {
				return fmt.Errorf("%w: consequence has candidate", ErrInvalidPlan)
			}
			if value.Decision != nil && *value.Decision != "retain" && !(value.DecisionMode == "content_candidate" && *value.Decision == "erase") {
				return fmt.Errorf("%w: invalid review decision", ErrInvalidPlan)
			}
		default:
			return fmt.Errorf("%w: invalid decision mode", ErrInvalidPlan)
		}
	}
	return nil
}

func validateEffects(plan Plan) error {
	previousFinding := ""
	for _, value := range plan.PlannedFindings {
		for _, id := range []string{value.IntegrityFindingID, value.TargetID, value.ResidentID, value.PipelineVersionID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		if value.ClaimID != nil {
			if err := validateID(*value.ClaimID); err != nil {
				return err
			}
		}
		if value.SourceContentErasureEventID != nil {
			if err := validateID(*value.SourceContentErasureEventID); err != nil {
				return err
			}
		}
		if value.DetailsContentID != nil || !digestPattern.MatchString(value.FindingFingerprint) || value.FindingKind != "required_provenance_erased" {
			return fmt.Errorf("%w: invalid finding", ErrInvalidPlan)
		}
		key := value.FindingFingerprint + "\x00" + value.IntegrityFindingID
		if previousFinding != "" && key <= previousFinding {
			return fmt.Errorf("%w: findings unordered", ErrInvalidPlan)
		}
		previousFinding = key
	}
	previousExisting := ""
	for _, value := range plan.ExistingFindingDependencies {
		for _, id := range []string{value.IntegrityFindingID, value.ClaimID, value.PipelineVersionID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		if !digestPattern.MatchString(value.FindingFingerprint) {
			return fmt.Errorf("%w: invalid existing finding", ErrInvalidPlan)
		}
		key := value.FindingFingerprint + "\x00" + value.IntegrityFindingID
		if previousExisting != "" && key <= previousExisting {
			return fmt.Errorf("%w: existing findings unordered", ErrInvalidPlan)
		}
		previousExisting = key
	}
	previousQ := ""
	for _, value := range plan.PlannedQuarantines {
		for _, id := range []string{value.StatusTransitionID, value.ClaimID, value.TriggerIntegrityFindingID, value.PipelineVersionID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		if value.FromStatus != "active" || value.ToStatus != "quarantined" || value.DecisionKind != "automatic" || value.ActorPrincipalID != nil || value.TriggerKind != "integrity_finding" || value.TriggerEventID != nil || value.TriggerEvidenceID != nil || value.TriggerClaimRelationID != nil || value.DecisionReasonCode != "structural_quarantine" || value.DecisionReasonContentID != nil {
			return fmt.Errorf("%w: invalid quarantine", ErrInvalidPlan)
		}
		if _, err := canonical.ParseCanonicalJSON([]byte(value.GateMetrics)); err != nil {
			return fmt.Errorf("%w: invalid gate metrics", ErrInvalidPlan)
		}
		key := value.ClaimID + "\x00" + value.StatusTransitionID
		if previousQ != "" && key <= previousQ {
			return fmt.Errorf("%w: quarantines unordered", ErrInvalidPlan)
		}
		previousQ = key
	}
	previousClaim := ""
	pairIDs := map[string]struct{}{}
	for _, value := range plan.ClaimIdentityErasures {
		for _, id := range []string{value.ClaimStatementErasureEventID, value.ClaimID, value.StatementContentID, value.ContentErasureEventID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		key := value.ClaimID + "\x00" + value.ClaimStatementErasureEventID
		if previousClaim != "" && key <= previousClaim {
			return fmt.Errorf("%w: claim erasures unordered", ErrInvalidPlan)
		}
		previousClaim = key
		if _, ok := pairIDs[value.ClaimID]; ok {
			return fmt.Errorf("%w: duplicate claim erasure", ErrInvalidPlan)
		}
		pairIDs[value.ClaimID] = struct{}{}
	}
	previousClaim = ""
	for _, value := range plan.ExistingClaimIdentityDependencies {
		for _, id := range []string{value.ClaimStatementErasureEventID, value.ClaimID, value.StatementContentID, value.ContentErasureEventID, value.CanonicalCommitID} {
			if err := validateID(id); err != nil {
				return err
			}
		}
		key := value.ClaimID + "\x00" + value.ClaimStatementErasureEventID
		if previousClaim != "" && key <= previousClaim {
			return fmt.Errorf("%w: claim dependencies unordered", ErrInvalidPlan)
		}
		previousClaim = key
		if _, ok := pairIDs[value.ClaimID]; ok {
			return fmt.Errorf("%w: claim both new and existing", ErrInvalidPlan)
		}
		pairIDs[value.ClaimID] = struct{}{}
	}
	previousRebuild := ""
	for _, value := range plan.Rebuilds {
		if err := validateID(value.ResidentID); err != nil {
			return err
		}
		if value.ProjectionName == "" || value.ProjectionVersion == "" || (value.ReasonCode != "content_erasure" && value.ReasonCode != "claim_state_change" && value.ReasonCode != "resident_lifecycle_change") {
			return fmt.Errorf("%w: invalid rebuild", ErrInvalidPlan)
		}
		key := value.ResidentID + "\x00" + value.ProjectionName + "\x00" + value.ProjectionVersion + "\x00" + value.ReasonCode
		if previousRebuild != "" && key <= previousRebuild {
			return fmt.Errorf("%w: rebuilds unordered", ErrInvalidPlan)
		}
		previousRebuild = key
	}
	previousBlocker := ""
	for _, value := range plan.Blockers {
		if value.TargetID != nil {
			if err := validateID(*value.TargetID); err != nil {
				return err
			}
		}
		if !strictSortedStrings(value.RequiredActionCodes) {
			return fmt.Errorf("%w: blocker actions unordered", ErrInvalidPlan)
		}
		if err := validateBlocker(value); err != nil {
			return err
		}
		key := value.Code + "\x00" + value.TargetKind + "\x00"
		if value.TargetID != nil {
			key += *value.TargetID
		}
		key += "\x00"
		if value.TargetField != nil {
			key += *value.TargetField
		}
		if previousBlocker != "" && key <= previousBlocker {
			return fmt.Errorf("%w: blockers unordered", ErrInvalidPlan)
		}
		previousBlocker = key
	}
	return nil
}

func validateScopeEffects(plan Plan) error {
	for _, id := range []*string{plan.RuntimeConfigEffect.ExpectedActiveResidentID, plan.RuntimeConfigEffect.ResultingActiveResidentID} {
		if id != nil {
			if err := validateID(*id); err != nil {
				return err
			}
		}
	}
	if plan.Scope == ScopeContent {
		if plan.ResidentTransition != nil || plan.RuntimeConfigEffect.ClearActiveResident {
			return fmt.Errorf("%w: content plan has resident effect", ErrInvalidPlan)
		}
		return nil
	}
	if len(plan.RequestedContentIDs) != 0 || plan.ResidentTransition == nil {
		return fmt.Errorf("%w: resident plan shape", ErrInvalidPlan)
	}
	r := plan.ResidentTransition
	for _, id := range []string{r.TransitionID, r.ResidentID, r.ActorPrincipalID} {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if r.ResidentID != plan.ResidentID || r.ActorPrincipalID != plan.ActorPrincipalID || r.ToStatus != "erased" || (r.FromStatus != "draft" && r.FromStatus != "active" && r.FromStatus != "archived") || r.ReasonContentID != nil || r.ReasonCode != plan.ReasonCode {
		return fmt.Errorf("%w: resident transition differs", ErrInvalidPlan)
	}
	if plan.RuntimeConfigEffect.ClearActiveResident {
		if plan.RuntimeConfigEffect.ExpectedActiveResidentID == nil || *plan.RuntimeConfigEffect.ExpectedActiveResidentID != plan.ResidentID || plan.RuntimeConfigEffect.ResultingActiveResidentID != nil {
			return fmt.Errorf("%w: invalid selection clear", ErrInvalidPlan)
		}
	} else if plan.RuntimeConfigEffect.ExpectedActiveResidentID != nil || plan.RuntimeConfigEffect.ResultingActiveResidentID != nil {
		return fmt.Errorf("%w: inactive selection effect has identities", ErrInvalidPlan)
	}
	return nil
}

func validatePlanRelationships(plan Plan) error {
	targetByContent := make(map[string]EffectiveTarget, len(plan.EffectiveTargets))
	targetByEvent := make(map[string]EffectiveTarget, len(plan.EffectiveTargets))
	requested := make(map[string]struct{}, len(plan.RequestedContentIDs))
	for _, id := range plan.RequestedContentIDs {
		requested[id] = struct{}{}
	}
	for _, target := range plan.EffectiveTargets {
		targetByContent[target.ContentID] = target
		targetByEvent[target.ErasureEventID] = target
		depth, _ := strconv.ParseUint(target.LineageDepth, 10, 64)
		if plan.Scope == ScopeContent {
			_, isRequested := requested[target.ContentID]
			if (depth == 0) != isRequested {
				return fmt.Errorf("%w: requested roots and depth-zero targets differ", ErrInvalidPlan)
			}
		}
		if depth > 0 {
			parent, ok := targetByEvent[nullableString(target.SourceErasureEventID)]
			if !ok {
				// validateTargets already proves the parent exists, but it may be
				// ordered later in the array. Resolve it after both maps are built.
				continue
			}
			parentDepth, _ := strconv.ParseUint(parent.LineageDepth, 10, 64)
			if parentDepth+1 != depth {
				return fmt.Errorf("%w: lineage depth is not parent depth plus one", ErrInvalidPlan)
			}
		}
	}
	// A blocked plan can intentionally omit a requested root that could not be
	// admitted (for example, a cross-resident content ID).  Ready plans must
	// still prove that every requested root is represented exactly at depth 0.
	if plan.Scope == ScopeContent && len(plan.Blockers) == 0 {
		for id := range requested {
			target, ok := targetByContent[id]
			if !ok || target.LineageDepth != "0" {
				return fmt.Errorf("%w: requested root missing from targets", ErrInvalidPlan)
			}
		}
	}

	impactByRuleRef := make(map[string]Impact, len(plan.Impacts))
	for _, impact := range plan.Impacts {
		if impact.RuleID != "projection_derived_state_v1" {
			if err := validateID(impact.ReferrerID); err != nil {
				return err
			}
		}
		impactByRuleRef[impact.RuleID+"\x00"+impact.ReferrerID] = impact
	}
	for _, target := range plan.EffectiveTargets {
		depth, _ := strconv.ParseUint(target.LineageDepth, 10, 64)
		if depth == 0 {
			continue
		}
		parent, ok := targetByEvent[nullableString(target.SourceErasureEventID)]
		if !ok {
			return fmt.Errorf("%w: derived source is not a target", ErrInvalidPlan)
		}
		parentDepth, _ := strconv.ParseUint(parent.LineageDepth, 10, 64)
		if parentDepth+1 != depth {
			return fmt.Errorf("%w: lineage depth is not parent depth plus one", ErrInvalidPlan)
		}
		if _, ok := impactByRuleRef["explicit_lineage_exact_bytes_v1\x00"+target.ContentID]; !ok {
			return fmt.Errorf("%w: derived target lacks must-erase impact", ErrInvalidPlan)
		}
	}
	for _, impact := range plan.Impacts {
		if impact.RuleID != "explicit_lineage_exact_bytes_v1" {
			continue
		}
		target, ok := targetByContent[impact.ReferrerID]
		if !ok || target.LineageDepth == "0" {
			return fmt.Errorf("%w: must-erase impact is not a derived target", ErrInvalidPlan)
		}
	}

	pairByClaim := make(map[string]ClaimIdentityErasure, len(plan.ClaimIdentityErasures))
	for _, pair := range plan.ClaimIdentityErasures {
		target, ok := targetByContent[pair.StatementContentID]
		if !ok || pair.ContentErasureEventID != target.ErasureEventID {
			return fmt.Errorf("%w: claim erasure is not bound to its target event", ErrInvalidPlan)
		}
		pairByClaim[pair.ClaimID] = pair
	}
	for _, dependency := range plan.ExistingClaimIdentityDependencies {
		_, targeted := targetByContent[dependency.StatementContentID]
		if plan.Scope != ScopeResident || targeted {
			return fmt.Errorf("%w: existing claim dependency is not historical resident state", ErrInvalidPlan)
		}
	}

	plannedFindingClaim := make(map[string]string, len(plan.PlannedFindings))
	existingFindingClaim := make(map[string]string, len(plan.ExistingFindingDependencies))
	statementFindingClaims := make(map[string]struct{}, len(plan.ClaimIdentityErasures))
	provenanceActions := map[string]map[string]struct{}{}
	for _, finding := range plan.PlannedFindings {
		if finding.ClaimID == nil || finding.ResidentID != plan.ResidentID || finding.PipelineVersionID != plan.IntegrityPipelineVersionID ||
			finding.FindingKind != string(integrity.FindingRequiredProvenanceErased) || finding.TargetKind != string(integrity.TargetClaim) ||
			finding.TargetID != *finding.ClaimID || finding.SourceContentErasureEventID == nil {
			return fmt.Errorf("%w: planned finding tuple differs", ErrInvalidPlan)
		}
		rule := integrity.RuleCode(finding.RuleCode)
		switch rule {
		case integrity.RuleClaimStatementErased:
			if finding.TargetField != "statement_content_id" {
				return fmt.Errorf("%w: statement finding field differs", ErrInvalidPlan)
			}
			pair, ok := pairByClaim[*finding.ClaimID]
			if !ok || pair.ContentErasureEventID != *finding.SourceContentErasureEventID {
				return fmt.Errorf("%w: finding is not bound to claim erasure", ErrInvalidPlan)
			}
			statementFindingClaims[*finding.ClaimID] = struct{}{}
		case integrity.RuleClaimQualifyingSupportErased:
			if finding.TargetField != "qualifying_support" {
				return fmt.Errorf("%w: support finding field differs", ErrInvalidPlan)
			}
			if _, ok := targetByEvent[*finding.SourceContentErasureEventID]; !ok {
				return fmt.Errorf("%w: support finding source is not a planned erasure", ErrInvalidPlan)
			}
		default:
			return fmt.Errorf("%w: unsupported planned finding rule", ErrInvalidPlan)
		}
		residentID, _ := canonical.ParseID(plan.ResidentID)
		claimID, _ := canonical.ParseID(*finding.ClaimID)
		sourceID, _ := canonical.ParseID(*finding.SourceContentErasureEventID)
		candidate, err := integrity.NewCandidate(integrity.CandidateInput{
			ResidentID: residentID, ClaimID: &claimID, Kind: integrity.FindingRequiredProvenanceErased,
			RuleCode: rule, TargetKind: integrity.TargetClaim,
			TargetID: claimID, TargetField: finding.TargetField, SourceContentErasureEventID: &sourceID,
			OccurredTZ: canonical.Timezone("UTC"),
		})
		if err != nil || finding.FindingFingerprint != "sha256:"+candidate.Fingerprint.Hex() {
			return fmt.Errorf("%w: planned finding fingerprint differs", ErrInvalidPlan)
		}
		plannedFindingClaim[finding.IntegrityFindingID] = *finding.ClaimID
		if provenanceActions[*finding.ClaimID] == nil {
			provenanceActions[*finding.ClaimID] = map[string]struct{}{}
		}
		provenanceActions[*finding.ClaimID]["record_integrity_finding"] = struct{}{}
	}
	for _, finding := range plan.ExistingFindingDependencies {
		rule := integrity.RuleCode(finding.RuleCode)
		kind := integrity.FindingKind(finding.FindingKind)
		targetField := ""
		switch rule {
		case integrity.RuleClaimStatementErased:
			targetField = "statement_content_id"
			if kind != integrity.FindingRequiredProvenanceErased || finding.SourceContentErasureEventID == nil {
				return fmt.Errorf("%w: existing statement finding tuple differs", ErrInvalidPlan)
			}
		case integrity.RuleClaimQualifyingSupportErased:
			targetField = "qualifying_support"
			if kind != integrity.FindingRequiredProvenanceErased || finding.SourceContentErasureEventID == nil {
				return fmt.Errorf("%w: existing support finding tuple differs", ErrInvalidPlan)
			}
		case integrity.RuleClaimQualifyingSupportUnresolvable:
			targetField = "qualifying_support"
			if kind != integrity.FindingProvenanceUnresolvable || finding.SourceContentErasureEventID != nil {
				return fmt.Errorf("%w: existing unresolvable finding tuple differs", ErrInvalidPlan)
			}
		default:
			return fmt.Errorf("%w: existing finding tuple differs", ErrInvalidPlan)
		}
		residentID, _ := canonical.ParseID(plan.ResidentID)
		claimID, _ := canonical.ParseID(finding.ClaimID)
		var sourceID *canonical.ID
		if finding.SourceContentErasureEventID != nil {
			parsed, _ := canonical.ParseID(*finding.SourceContentErasureEventID)
			sourceID = &parsed
		}
		candidate, err := integrity.NewCandidate(integrity.CandidateInput{
			ResidentID: residentID, ClaimID: &claimID, Kind: kind,
			RuleCode: rule, TargetKind: integrity.TargetClaim,
			TargetID: claimID, TargetField: targetField, SourceContentErasureEventID: sourceID,
			OccurredTZ: canonical.Timezone("UTC"),
		})
		if err != nil || finding.FindingFingerprint != "sha256:"+candidate.Fingerprint.Hex() {
			return fmt.Errorf("%w: existing finding fingerprint differs", ErrInvalidPlan)
		}
		existingFindingClaim[finding.IntegrityFindingID] = finding.ClaimID
	}
	for claim := range pairByClaim {
		if _, ok := statementFindingClaims[claim]; !ok {
			return fmt.Errorf("%w: claim identity erasure lacks provenance finding", ErrInvalidPlan)
		}
	}
	for _, quarantine := range plan.PlannedQuarantines {
		if quarantine.PipelineVersionID != plan.MemoryStatusPipelineVersionID || quarantine.GateMetrics != `{"reason":"required_provenance_erased"}` {
			return fmt.Errorf("%w: quarantine pipeline or gate metrics differs", ErrInvalidPlan)
		}
		claim, ok := plannedFindingClaim[quarantine.TriggerIntegrityFindingID]
		if !ok {
			claim, ok = existingFindingClaim[quarantine.TriggerIntegrityFindingID]
		}
		if !ok || claim != quarantine.ClaimID {
			return fmt.Errorf("%w: quarantine trigger does not target its claim", ErrInvalidPlan)
		}
		if provenanceActions[claim] == nil {
			provenanceActions[claim] = map[string]struct{}{}
		}
		provenanceActions[claim]["quarantine_claim"] = struct{}{}
	}
	seenProvenance := map[string]struct{}{}
	for _, impact := range plan.Impacts {
		if impact.RuleID != "claim_provenance_break_v1" {
			continue
		}
		wantSet, ok := provenanceActions[impact.ReferrerID]
		if !ok {
			return fmt.Errorf("%w: provenance impact has no planned effect", ErrInvalidPlan)
		}
		want := make([]string, 0, len(wantSet))
		for action := range wantSet {
			want = append(want, action)
		}
		slices.Sort(want)
		if !slices.Equal(impact.Actions, want) {
			return fmt.Errorf("%w: provenance actions differ", ErrInvalidPlan)
		}
		seenProvenance[impact.ReferrerID] = struct{}{}
	}
	for claim := range provenanceActions {
		if _, ok := seenProvenance[claim]; !ok {
			return fmt.Errorf("%w: planned provenance effect lacks impact", ErrInvalidPlan)
		}
	}

	projectionVersions := map[string]string{}
	projectionReasons := map[string]map[string]struct{}{}
	for _, rebuild := range plan.Rebuilds {
		if version, exists := projectionVersions[rebuild.ProjectionName]; exists && version != rebuild.ProjectionVersion {
			return fmt.Errorf("%w: projection rebuild versions differ", ErrInvalidPlan)
		}
		projectionVersions[rebuild.ProjectionName] = rebuild.ProjectionVersion
		if projectionReasons[rebuild.ProjectionName] == nil {
			projectionReasons[rebuild.ProjectionName] = map[string]struct{}{}
		}
		projectionReasons[rebuild.ProjectionName][rebuild.ReasonCode] = struct{}{}
	}
	for name, reasons := range projectionReasons {
		if _, ok := reasons["content_erasure"]; !ok {
			return fmt.Errorf("%w: projection lacks content erasure rebuild", ErrInvalidPlan)
		}
		if len(plan.PlannedQuarantines) != 0 {
			if _, ok := reasons["claim_state_change"]; !ok {
				return fmt.Errorf("%w: projection lacks claim-state rebuild", ErrInvalidPlan)
			}
		}
		if plan.Scope == ScopeResident {
			if _, ok := reasons["resident_lifecycle_change"]; !ok {
				return fmt.Errorf("%w: projection lacks resident lifecycle rebuild", ErrInvalidPlan)
			}
		}
		if _, ok := impactByRuleRef["projection_derived_state_v1\x00"+name]; !ok {
			return fmt.Errorf("%w: projection rebuild lacks impact", ErrInvalidPlan)
		}
	}
	for _, impact := range plan.Impacts {
		if impact.RuleID == "projection_derived_state_v1" {
			if _, ok := projectionVersions[impact.ReferrerID]; !ok {
				return fmt.Errorf("%w: projection impact lacks rebuild", ErrInvalidPlan)
			}
		}
	}

	lifecycle, hasLifecycle := impactByRuleRef["resident_lifecycle_erase_v1\x00"+plan.ResidentID]
	lifecycleCount := 0
	for _, impact := range plan.Impacts {
		if impact.RuleID == "resident_lifecycle_erase_v1" {
			lifecycleCount++
			if impact.ReferrerID != plan.ResidentID {
				return fmt.Errorf("%w: lifecycle impact targets another resident", ErrInvalidPlan)
			}
		}
	}
	if plan.Scope == ScopeContent {
		if hasLifecycle || lifecycleCount != 0 {
			return fmt.Errorf("%w: content plan has lifecycle impact", ErrInvalidPlan)
		}
	} else {
		if !hasLifecycle || lifecycleCount != 1 {
			return fmt.Errorf("%w: resident plan lacks lifecycle impact", ErrInvalidPlan)
		}
		clearAction := slices.Contains(lifecycle.Actions, "clear_runtime_selection")
		if clearAction != plan.RuntimeConfigEffect.ClearActiveResident {
			return fmt.Errorf("%w: lifecycle selection action differs", ErrInvalidPlan)
		}
	}
	return nil
}

func nilCollections(p Plan) bool {
	return p.RequestedContentIDs == nil || p.EffectiveTargets == nil || p.Impacts == nil || p.PlannedFindings == nil || p.ExistingFindingDependencies == nil || p.PlannedQuarantines == nil || p.ClaimIdentityErasures == nil || p.ExistingClaimIdentityDependencies == nil || p.Rebuilds == nil || p.Blockers == nil
}
func validateID(value string) error {
	if _, err := canonical.ParseID(value); err != nil {
		return fmt.Errorf("%w: invalid ID %q", ErrInvalidPlan, value)
	}
	return nil
}
func positiveDecimal(v string) bool { return v != "" && v != "0" && nonNegativeDecimal(v) }
func nonNegativeDecimal(v string) bool {
	if v == "0" {
		return true
	}
	if v == "" || v[0] < '1' || v[0] > '9' {
		return false
	}
	for _, r := range v[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
func strictSortedStrings(v []string) bool { return slices.IsSorted(v) && !hasDuplicate(v) }
func hasDuplicate(v []string) bool {
	for i := 1; i < len(v); i++ {
		if v[i] == v[i-1] {
			return true
		}
	}
	return false
}
func compareTarget(a, b EffectiveTarget) int {
	ad, _ := strconv.ParseUint(a.LineageDepth, 10, 64)
	bd, _ := strconv.ParseUint(b.LineageDepth, 10, 64)
	if ad < bd {
		return -1
	}
	if ad > bd {
		return 1
	}
	if c := compareNullable(a.SourceErasureEventID, b.SourceErasureEventID); c != 0 {
		return c
	}
	return strings.Compare(a.ContentID, b.ContentID)
}

func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
