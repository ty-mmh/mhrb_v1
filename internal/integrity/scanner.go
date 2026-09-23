package integrity

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	FindingFingerprintDomain = "mahoroba:integrity-finding:v1"
	IntegrityPipelineKind    = "integrity_check"
	IntegrityPipelineVersion = "integrity-check-v1"
)

var (
	ErrInvalidCandidate   = errors.New("integrity: invalid finding candidate")
	ErrCandidateConflict  = errors.New("integrity: finding candidate semantic conflict")
	ErrFindingConflict    = errors.New("integrity: persisted finding semantic conflict")
	ErrQuarantineConflict = errors.New("integrity: structural quarantine conflict")
)

type FindingKind string

const (
	FindingRequiredProvenanceErased FindingKind = "required_provenance_erased"
	FindingProvenanceUnresolvable   FindingKind = "provenance_unresolvable"
	FindingCanonicalInvariant       FindingKind = "canonical_invariant_violation"
)

func (kind FindingKind) Validate() error {
	switch kind {
	case FindingRequiredProvenanceErased, FindingProvenanceUnresolvable, FindingCanonicalInvariant:
		return nil
	default:
		return fmt.Errorf("%w: unknown finding kind %q", ErrInvalidCandidate, kind)
	}
}

type RuleCode string

const (
	RuleClaimStatementErased               RuleCode = "claim_statement_erased"
	RuleClaimQualifyingSupportErased       RuleCode = "claim_qualifying_support_erased"
	RuleClaimQualifyingSupportUnresolvable RuleCode = "claim_qualifying_support_unresolvable"
	RuleGenerationInputSourceMissing       RuleCode = "generation_input_source_missing"
	RuleActiveRequiredRevisionErased       RuleCode = "active_required_revision_erased"
	RuleRunningAttemptInputErased          RuleCode = "running_attempt_input_erased"
	RuleCancellationEnvelopeUnresolvable   RuleCode = "mandatory_work_cancellation_envelope_unresolvable"
)

type TargetKind string

const (
	TargetClaim            TargetKind = "claim"
	TargetGenerationInput  TargetKind = "generation_input"
	TargetResidentRevision TargetKind = "resident_revision"
	TargetEvent            TargetKind = "event"
)

type ruleContract struct {
	kind           FindingKind
	targetKind     TargetKind
	targetField    string
	claimTarget    bool
	sourceRequired bool
}

var ruleContracts = map[RuleCode]ruleContract{
	RuleClaimStatementErased: {
		kind: FindingRequiredProvenanceErased, targetKind: TargetClaim,
		targetField: "statement_content_id", claimTarget: true, sourceRequired: true,
	},
	RuleClaimQualifyingSupportErased: {
		kind: FindingRequiredProvenanceErased, targetKind: TargetClaim,
		targetField: "qualifying_support", claimTarget: true, sourceRequired: true,
	},
	RuleClaimQualifyingSupportUnresolvable: {
		kind: FindingProvenanceUnresolvable, targetKind: TargetClaim,
		targetField: "qualifying_support", claimTarget: true,
	},
	RuleGenerationInputSourceMissing: {
		kind: FindingProvenanceUnresolvable, targetKind: TargetGenerationInput,
		targetField: "source",
	},
	RuleActiveRequiredRevisionErased: {
		kind: FindingCanonicalInvariant, targetKind: TargetResidentRevision,
		targetField: "content_id", sourceRequired: true,
	},
	RuleRunningAttemptInputErased: {
		kind: FindingCanonicalInvariant, targetKind: TargetGenerationInput,
		targetField: "content_id", sourceRequired: true,
	},
	RuleCancellationEnvelopeUnresolvable: {
		kind: FindingProvenanceUnresolvable, targetKind: TargetEvent,
		targetField: "cancellation_envelope",
	},
}

// CandidateInput is the non-sensitive logical tuple returned by a captured
// scanner read. It deliberately cannot carry content bytes, content hashes,
// salts, commitments, or diagnostic prose.
type CandidateInput struct {
	ResidentID                  canonical.ID
	ClaimID                     *canonical.ID
	Kind                        FindingKind
	RuleCode                    RuleCode
	TargetKind                  TargetKind
	TargetID                    canonical.ID
	TargetField                 string
	SourceContentErasureEventID *canonical.ID
	OccurredAt                  canonical.Instant
	OccurredTZ                  canonical.Timezone
}

func (input CandidateInput) Validate() error {
	if err := input.ResidentID.Validate(); err != nil {
		return fmt.Errorf("%w: resident ID: %v", ErrInvalidCandidate, err)
	}
	if err := input.TargetID.Validate(); err != nil {
		return fmt.Errorf("%w: target ID: %v", ErrInvalidCandidate, err)
	}
	if err := input.Kind.Validate(); err != nil {
		return err
	}
	if err := input.OccurredTZ.Validate(); err != nil {
		return fmt.Errorf("%w: occurred timezone: %v", ErrInvalidCandidate, err)
	}
	contract, exists := ruleContracts[input.RuleCode]
	if !exists {
		return fmt.Errorf("%w: unknown rule code %q", ErrInvalidCandidate, input.RuleCode)
	}
	if input.Kind != contract.kind || input.TargetKind != contract.targetKind || input.TargetField != contract.targetField {
		return fmt.Errorf("%w: rule %s does not match finding kind or target tuple", ErrInvalidCandidate, input.RuleCode)
	}
	if contract.claimTarget {
		if input.ClaimID == nil || *input.ClaimID != input.TargetID {
			return fmt.Errorf("%w: rule %s requires claim_id to equal target_id", ErrInvalidCandidate, input.RuleCode)
		}
	} else if input.ClaimID != nil {
		return fmt.Errorf("%w: rule %s forbids claim_id", ErrInvalidCandidate, input.RuleCode)
	}
	if input.ClaimID != nil {
		if err := input.ClaimID.Validate(); err != nil {
			return fmt.Errorf("%w: claim ID: %v", ErrInvalidCandidate, err)
		}
	}
	if contract.sourceRequired != (input.SourceContentErasureEventID != nil) {
		return fmt.Errorf("%w: rule %s has invalid source erasure event presence", ErrInvalidCandidate, input.RuleCode)
	}
	if input.SourceContentErasureEventID != nil {
		if err := input.SourceContentErasureEventID.Validate(); err != nil {
			return fmt.Errorf("%w: source erasure event ID: %v", ErrInvalidCandidate, err)
		}
	}
	return nil
}

// Candidate is a validated logical finding plus its v1 fingerprint.
type Candidate struct {
	CandidateInput
	Fingerprint canonical.Digest
}

// RequiresQuarantine reports whether Apply must preallocate a status
// transition ID so the Writer can quarantine an active structural claim in
// the same Canonical unit of work as its finding.
func (candidate Candidate) RequiresQuarantine() bool {
	contract, exists := ruleContracts[candidate.RuleCode]
	return exists && contract.claimTarget
}

// ReadinessBlocking identifies rules whose current predicate prevents service
// activation. Historical finding rows are intentionally irrelevant: callers
// evaluate this against a fresh scanner snapshot at the final durable head.
func (candidate Candidate) ReadinessBlocking() bool {
	switch candidate.RuleCode {
	case RuleActiveRequiredRevisionErased,
		RuleRunningAttemptInputErased,
		RuleCancellationEnvelopeUnresolvable:
		return true
	default:
		return false
	}
}

func (candidate Candidate) Validate() error {
	computed, err := NewCandidate(candidate.CandidateInput)
	if err != nil {
		return err
	}
	if computed.Fingerprint != candidate.Fingerprint {
		return fmt.Errorf("%w: fingerprint does not match the v1 logical tuple", ErrInvalidCandidate)
	}
	return nil
}

// NewCandidate validates the catalog tuple and derives the fingerprint. The
// caller cannot supply a digest, so Apply can always recompute it independently.
func NewCandidate(input CandidateInput) (Candidate, error) {
	if err := input.Validate(); err != nil {
		return Candidate{}, err
	}
	fingerprint, err := FingerprintV1(input)
	if err != nil {
		return Candidate{}, err
	}
	return Candidate{CandidateInput: input, Fingerprint: fingerprint}, nil
}

// FingerprintV1 hashes an exact JCS object behind the specified domain and a
// NUL separator, matching the repository's other domain-separated hashes.
func FingerprintV1(input CandidateInput) (canonical.Digest, error) {
	if err := input.Validate(); err != nil {
		return canonical.Digest{}, err
	}
	envelope, err := canonical.MarshalCanonical(struct {
		Kind                        FindingKind   `json:"kind"`
		RuleCode                    RuleCode      `json:"rule_code"`
		ResidentID                  canonical.ID  `json:"resident_id"`
		TargetKind                  TargetKind    `json:"target_kind"`
		TargetID                    canonical.ID  `json:"target_id"`
		TargetField                 string        `json:"target_field"`
		SourceContentErasureEventID *canonical.ID `json:"source_content_erasure_event_id"`
	}{
		Kind: input.Kind, RuleCode: input.RuleCode, ResidentID: input.ResidentID,
		TargetKind: input.TargetKind, TargetID: input.TargetID, TargetField: input.TargetField,
		SourceContentErasureEventID: input.SourceContentErasureEventID,
	})
	if err != nil {
		return canonical.Digest{}, fmt.Errorf("integrity: encode finding fingerprint: %w", err)
	}
	hasher := sha256.New()
	_, _ = io.WriteString(hasher, FindingFingerprintDomain)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(envelope.Bytes())
	return canonical.DigestFromBytes(hasher.Sum(nil))
}

// SemanticEqual compares exactly the fields that define finding identity.
// Occurrence/recording time, commit, pipeline and details are deliberately not
// part of this comparison.
func SemanticEqual(left, right Candidate) bool {
	return left.Fingerprint == right.Fingerprint &&
		left.ResidentID == right.ResidentID && equalOptionalID(left.ClaimID, right.ClaimID) &&
		left.Kind == right.Kind && left.RuleCode == right.RuleCode &&
		left.TargetKind == right.TargetKind && left.TargetID == right.TargetID &&
		left.TargetField == right.TargetField &&
		equalOptionalID(left.SourceContentErasureEventID, right.SourceContentErasureEventID)
}

func equalOptionalID(left, right *canonical.ID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type ScanSnapshot struct {
	CapturedHead canonical.Head
	Candidates   []CandidateInput
}

// ScanSource must capture head and candidate rows in one read-only snapshot.
type ScanSource interface {
	CaptureIntegrityCandidates(context.Context) (ScanSnapshot, error)
}

type Scanner struct {
	source ScanSource
}

func NewScanner(source ScanSource) *Scanner { return &Scanner{source: source} }

type ScanResult struct {
	CapturedHead canonical.Head
	Candidates   []Candidate
}

type PlannedFinding struct {
	FindingID canonical.ID
	Candidate Candidate
	// QuarantineTransitionID is mandatory for structural claim rules. Apply
	// consumes it only when the claim is active, but carrying it for every
	// structural candidate lets the Writer decide from current Canonical state
	// without trusting a stale scanner-side status observation.
	QuarantineTransitionID *canonical.ID
}

type RecordFindings struct {
	ResidentID                    canonical.ID
	CapturedHead                  canonical.Head
	PipelineVersionID             canonical.ID
	MemoryStatusPipelineVersionID *canonical.ID
	Findings                      []PlannedFinding
}

func (value RecordFindings) Validate() error {
	if err := value.ResidentID.Validate(); err != nil {
		return err
	}
	if err := value.CapturedHead.Validate(); err != nil {
		return err
	}
	if err := value.PipelineVersionID.Validate(); err != nil {
		return err
	}
	if len(value.Findings) == 0 {
		return fmt.Errorf("%w: at least one finding is required", ErrInvalidCandidate)
	}
	ids := make(map[canonical.ID]struct{}, len(value.Findings)*2)
	claimTransitions := make(map[canonical.ID]canonical.ID, len(value.Findings))
	requiresMemoryStatus := false
	for _, planned := range value.Findings {
		if err := planned.FindingID.Validate(); err != nil {
			return err
		}
		if _, duplicate := ids[planned.FindingID]; duplicate {
			return fmt.Errorf("%w: duplicate planned finding ID", ErrInvalidCandidate)
		}
		ids[planned.FindingID] = struct{}{}
		if err := planned.Candidate.Validate(); err != nil {
			return err
		}
		if planned.Candidate.ResidentID != value.ResidentID {
			return fmt.Errorf("%w: candidate belongs to another resident", ErrInvalidCandidate)
		}
		contract := ruleContracts[planned.Candidate.RuleCode]
		if contract.claimTarget {
			requiresMemoryStatus = true
			if planned.QuarantineTransitionID == nil {
				return fmt.Errorf("%w: structural claim finding requires a quarantine transition ID", ErrInvalidCandidate)
			}
			if err := planned.QuarantineTransitionID.Validate(); err != nil {
				return err
			}
			if existing, duplicateClaim := claimTransitions[planned.Candidate.TargetID]; duplicateClaim {
				if existing != *planned.QuarantineTransitionID {
					return fmt.Errorf("%w: structural rules for one claim must share one quarantine transition", ErrInvalidCandidate)
				}
			} else {
				if _, duplicate := ids[*planned.QuarantineTransitionID]; duplicate {
					return fmt.Errorf("%w: duplicate planned mutation ID", ErrInvalidCandidate)
				}
				ids[*planned.QuarantineTransitionID] = struct{}{}
				claimTransitions[planned.Candidate.TargetID] = *planned.QuarantineTransitionID
			}
		} else if planned.QuarantineTransitionID != nil {
			return fmt.Errorf("%w: non-claim finding cannot plan a quarantine", ErrInvalidCandidate)
		}
	}
	if requiresMemoryStatus != (value.MemoryStatusPipelineVersionID != nil) {
		return fmt.Errorf("%w: structural findings require exactly one memory-status-v1 dependency", ErrInvalidCandidate)
	}
	if value.MemoryStatusPipelineVersionID != nil {
		if err := value.MemoryStatusPipelineVersionID.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type RecordedFinding struct {
	Fingerprint canonical.Digest
	FindingID   canonical.ID
	Created     bool
	Commit      canonical.CommitMetadata
}

type RecordFindingsResult struct {
	Existing    []RecordedFinding
	Created     []RecordedFinding
	Quarantined []RecordedQuarantine
}

type RecordedQuarantine struct {
	ClaimID            canonical.ID
	FindingID          canonical.ID
	StatusTransitionID canonical.ID
}

func (scanner *Scanner) Scan(ctx context.Context) (ScanResult, error) {
	if scanner == nil || scanner.source == nil {
		return ScanResult{}, errors.New("integrity: scanner source is required")
	}
	if ctx == nil {
		return ScanResult{}, errors.New("integrity: nil scan context")
	}
	snapshot, err := scanner.source.CaptureIntegrityCandidates(ctx)
	if err != nil {
		return ScanResult{}, fmt.Errorf("integrity: capture scan snapshot: %w", err)
	}
	if err := snapshot.CapturedHead.Validate(); err != nil {
		return ScanResult{}, fmt.Errorf("integrity: invalid captured head: %w", err)
	}

	candidates := make([]Candidate, 0, len(snapshot.Candidates))
	byFingerprint := make(map[canonical.Digest]int, len(snapshot.Candidates))
	for _, input := range snapshot.Candidates {
		candidate, err := NewCandidate(input)
		if err != nil {
			return ScanResult{}, err
		}
		if index, exists := byFingerprint[candidate.Fingerprint]; exists {
			if !SemanticEqual(candidates[index], candidate) {
				return ScanResult{}, ErrCandidateConflict
			}
			continue
		}
		byFingerprint[candidate.Fingerprint] = len(candidates)
		candidates = append(candidates, candidate)
	}
	slices.SortFunc(candidates, func(left, right Candidate) int {
		if left.ResidentID.String() != right.ResidentID.String() {
			if left.ResidentID.String() < right.ResidentID.String() {
				return -1
			}
			return 1
		}
		if left.Fingerprint.Hex() < right.Fingerprint.Hex() {
			return -1
		}
		if left.Fingerprint.Hex() > right.Fingerprint.Hex() {
			return 1
		}
		return 0
	})
	return ScanResult{CapturedHead: snapshot.CapturedHead, Candidates: candidates}, nil
}
