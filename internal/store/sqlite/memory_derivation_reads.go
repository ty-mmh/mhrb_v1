package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type derivationEvidenceCandidate struct {
	sourceIndex int
	id          canonical.ID
	eventID     canonical.ID
	weight      int64
}

func (r *CanonicalRepository) PrepareMemoryDerivation(
	ctx context.Context,
	residentID canonical.ID,
	purpose domain.GenerationPurpose,
	sourceIDs []canonical.ID,
) (domain.DerivedClaimPreparation, error) {
	if purpose != domain.GenerationPurposeMemoryAbstraction && purpose != domain.GenerationPurposeMemoryDifferentiation {
		return domain.DerivedClaimPreparation{}, errors.New("sqlite: unsupported memory derivation purpose")
	}
	if (purpose == domain.GenerationPurposeMemoryAbstraction && (len(sourceIDs) < 2 || len(sourceIDs) > 8)) ||
		(purpose == domain.GenerationPurposeMemoryDifferentiation && len(sourceIDs) != 1) {
		return domain.DerivedClaimPreparation{}, errors.New("sqlite: invalid memory derivation source cardinality")
	}
	seen := make(map[canonical.ID]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if err := id.Validate(); err != nil {
			return domain.DerivedClaimPreparation{}, err
		}
		if _, duplicate := seen[id]; duplicate {
			return domain.DerivedClaimPreparation{}, errors.New("sqlite: memory derivation sources must be unique")
		}
		seen[id] = struct{}{}
	}
	resident, err := r.Resident(ctx, residentID)
	if err != nil {
		return domain.DerivedClaimPreparation{}, err
	}
	if resident.Status != "active" {
		return domain.DerivedClaimPreparation{}, errors.New("sqlite: memory derivation requires active resident")
	}
	policy, _, err := memory.ParsePolicy([]byte(resident.MemoryPolicy))
	if err != nil {
		return domain.DerivedClaimPreparation{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return domain.DerivedClaimPreparation{}, err
	}
	kind, version := "memory_abstraction", domain.MemoryAbstractionPipelineVersion
	if purpose == domain.GenerationPurposeMemoryDifferentiation {
		kind, version = "memory_differentiation", domain.MemoryDifferentiationPipelineVersion
	}
	pipeline, err := r.PipelineVersion(ctx, kind, version)
	if err != nil {
		return domain.DerivedClaimPreparation{}, err
	}
	result := domain.DerivedClaimPreparation{
		Resident: resident, PolicyContent: resident.MemoryPolicy, PipelineVersionID: pipeline.ID,
		Sources: make([]domain.DerivedClaimSourceContext, len(sourceIDs)),
	}
	var identitySubject, identityPerspective, identityKind string
	allEvidence := make([]derivationEvidenceCandidate, 0, len(sourceIDs)*2)
	for index, sourceID := range sourceIDs {
		eligible, err := loadEligibleClaimStatement(ctx, r.store.reader, residentID, sourceID)
		if err != nil {
			return domain.DerivedClaimPreparation{}, fmt.Errorf("sqlite: load derivation source %s: %w", sourceID, err)
		}
		var status string
		err = r.store.reader.QueryRowContext(ctx, `SELECT
			COALESCE((SELECT transition.to_status FROM claim_status_transitions transition
				JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
				WHERE transition.claim_id = claim.claim_id
				ORDER BY commit_row.commit_seq DESC, transition.status_transition_id DESC LIMIT 1), 'active')
			FROM claims claim WHERE claim.claim_id = ?`, sourceID.String()).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.DerivedClaimPreparation{}, fmt.Errorf("%w: derivation source %s is inactive, erased, or outside resident", domain.ErrClaimSourceIneligible, sourceID)
			}
			return domain.DerivedClaimPreparation{}, fmt.Errorf("sqlite: load derivation source %s: %w", sourceID, err)
		}
		if status != "active" {
			return domain.DerivedClaimPreparation{}, fmt.Errorf("%w: derivation source %s is inactive, erased, or outside resident", domain.ErrClaimSourceIneligible, sourceID)
		}
		statement := string(eligible.Statement)
		kindValue := string(eligible.Kind)
		if index == 0 {
			identitySubject, identityPerspective, identityKind = eligible.SubjectID.String(), eligible.PerspectiveID.String(), kindValue
		} else if eligible.SubjectID.String() != identitySubject || eligible.PerspectiveID.String() != identityPerspective || kindValue != identityKind {
			return domain.DerivedClaimPreparation{}, errors.New("sqlite: derivation source identity mismatch")
		}
		result.Sources[index] = domain.DerivedClaimSourceContext{ClaimID: sourceID, Statement: statement}
		rows, err := r.store.reader.QueryContext(ctx, `SELECT evidence.evidence_id, evidence.event_id, evidence.weight
			FROM claim_evidence evidence
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = evidence.canonical_commit_id
			WHERE evidence.claim_id = ? AND evidence.derivation = 'extracted'
			ORDER BY evidence.weight DESC, commit_row.commit_seq, evidence.evidence_id`, sourceID.String())
		if err != nil {
			return domain.DerivedClaimPreparation{}, err
		}
		for rows.Next() {
			var evidenceRaw, eventRaw string
			var weight int64
			if err := rows.Scan(&evidenceRaw, &eventRaw, &weight); err != nil {
				rows.Close()
				return domain.DerivedClaimPreparation{}, err
			}
			evidenceID, err := canonical.ParseID(evidenceRaw)
			if err != nil {
				rows.Close()
				return domain.DerivedClaimPreparation{}, err
			}
			eventID, err := canonical.ParseID(eventRaw)
			if err != nil {
				rows.Close()
				return domain.DerivedClaimPreparation{}, err
			}
			allEvidence = append(allEvidence, derivationEvidenceCandidate{index, evidenceID, eventID, weight})
		}
		if err := rows.Close(); err != nil {
			return domain.DerivedClaimPreparation{}, err
		}
	}
	sort.SliceStable(allEvidence, func(left, right int) bool {
		if allEvidence[left].weight != allEvidence[right].weight {
			return allEvidence[left].weight > allEvidence[right].weight
		}
		return allEvidence[left].id.String() < allEvidence[right].id.String()
	})
	seenEvents := make(map[canonical.ID]struct{}, len(allEvidence))
	perSource := make([]int, len(sourceIDs))
	selected := 0
	for _, candidate := range allEvidence {
		if selected == domain.MaximumInheritedEvidence {
			break
		}
		if perSource[candidate.sourceIndex] == 2 {
			continue
		}
		if _, duplicate := seenEvents[candidate.eventID]; duplicate {
			continue
		}
		seenEvents[candidate.eventID] = struct{}{}
		perSource[candidate.sourceIndex]++
		selected++
		result.Sources[candidate.sourceIndex].EvidenceIDs = append(
			result.Sources[candidate.sourceIndex].EvidenceIDs, candidate.id,
		)
	}
	if selected == 0 {
		return domain.DerivedClaimPreparation{}, errors.New("sqlite: derivation sources have no eligible Canonical evidence")
	}
	return result, nil
}

var _ domain.MemoryDerivationRepository = (*CanonicalRepository)(nil)
