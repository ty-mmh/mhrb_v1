package integrityrun

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

func TestM7IntegrityRunGroupsResidentsAndSecondScanIsIdempotent(t *testing.T) {
	residentA := integrityRunID(t, "01H00000000000000000000001")
	residentB := integrityRunID(t, "01H00000000000000000000002")
	fixture := newIntegrityRunFixture(t, []integrity.CandidateInput{
		integrityRunCandidate(t, residentB, "01H00000000000000000000012"),
		integrityRunCandidate(t, residentA, "01H00000000000000000000011"),
	})

	first, err := fixture.service.Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExistingFindings != 0 || first.CreatedFindings != 2 || first.CreatedQuarantines != 0 {
		t.Fatalf("first counts = existing:%d created:%d quarantines:%d",
			first.ExistingFindings, first.CreatedFindings, first.CreatedQuarantines)
	}
	if len(first.CanonicalCommits) != 3 {
		t.Fatalf("first commit count = %d, want pipeline + two resident commits", len(first.CanonicalCommits))
	}
	if first.CapturedHead.Head.CommitSeq.Int64() != 1 || first.ResultHead.Head.CommitSeq.Int64() != 3 {
		t.Fatalf("first heads = %d -> %d, want 1 -> 3",
			first.CapturedHead.Head.CommitSeq.Int64(), first.ResultHead.Head.CommitSeq.Int64())
	}
	if got := fixture.writer.residentScopes; len(got) != 2 || got[0] != residentA || got[1] != residentB {
		t.Fatalf("resident apply order = %v, want lexical A then B", got)
	}

	fixture.writer.residentScopes = nil
	second, err := fixture.service.Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if second.ExistingFindings != 2 || second.CreatedFindings != 0 || second.CreatedQuarantines != 0 {
		t.Fatalf("second counts = existing:%d created:%d quarantines:%d",
			second.ExistingFindings, second.CreatedFindings, second.CreatedQuarantines)
	}
	if len(second.CanonicalCommits) != 3 {
		t.Fatalf("idempotent second scan reported %d existing commits, want 3", len(second.CanonicalCommits))
	}
	for _, commit := range second.CanonicalCommits {
		if commit.Disposition != DispositionExisting {
			t.Fatalf("idempotent commit disposition = %q", commit.Disposition)
		}
	}
	if second.CapturedHead != first.ResultHead || second.ResultHead != first.ResultHead {
		t.Fatalf("idempotent heads changed: captured=%+v result=%+v", second.CapturedHead, second.ResultHead)
	}
}

func TestM7IntegrityRunResidentFilterDoesNotApplyOtherResidents(t *testing.T) {
	residentA := integrityRunID(t, "01H00000000000000000000001")
	residentB := integrityRunID(t, "01H00000000000000000000002")
	fixture := newIntegrityRunFixture(t, []integrity.CandidateInput{
		integrityRunCandidate(t, residentA, "01H00000000000000000000011"),
		integrityRunCandidate(t, residentB, "01H00000000000000000000012"),
	})

	result, err := fixture.service.Run(context.Background(), &residentB)
	if err != nil {
		t.Fatal(err)
	}
	if result.CreatedFindings != 1 || len(fixture.writer.findings) != 1 {
		t.Fatalf("filtered created findings = %d, persisted=%d", result.CreatedFindings, len(fixture.writer.findings))
	}
	if got := fixture.writer.residentScopes; len(got) != 1 || got[0] != residentB {
		t.Fatalf("filtered resident scopes = %v, want only %s", got, residentB)
	}
}

func TestM7IntegrityRunPlansStructuralQuarantineAndReevaluatesReadiness(t *testing.T) {
	residentID := integrityRunID(t, "01H00000000000000000000001")
	claimID := integrityRunID(t, "01H00000000000000000000021")
	sourceID := integrityRunID(t, "01H00000000000000000000031")
	revisionID := integrityRunID(t, "01H00000000000000000000041")
	fixture := newIntegrityRunFixture(t, []integrity.CandidateInput{
		{
			ResidentID: residentID, ClaimID: &claimID,
			Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimStatementErased,
			TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "statement_content_id",
			SourceContentErasureEventID: &sourceID,
			OccurredAt:                  10, OccurredTZ: canonical.MustTimezone("UTC"),
		},
		{
			ResidentID: residentID,
			Kind:       integrity.FindingCanonicalInvariant, RuleCode: integrity.RuleActiveRequiredRevisionErased,
			TargetKind: integrity.TargetResidentRevision, TargetID: revisionID, TargetField: "content_id",
			SourceContentErasureEventID: &sourceID,
			OccurredAt:                  10, OccurredTZ: canonical.MustTimezone("UTC"),
		},
	})

	result, err := fixture.service.Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.CreatedFindings != 2 || result.CreatedQuarantines != 1 || !result.ReadinessBlocked {
		t.Fatalf("structural result = created:%d quarantines:%d blocked:%v",
			result.CreatedFindings, result.CreatedQuarantines, result.ReadinessBlocked)
	}
	if len(result.CanonicalCommits) != 2 ||
		!result.CanonicalCommits[1].Effects.IntegrityFindingRecorded ||
		!result.CanonicalCommits[1].Effects.ClaimStatusQuarantined {
		t.Fatalf("structural commit effects = %#v", result.CanonicalCommits)
	}
}

func TestM7IntegrityRunSharesOneQuarantineTransitionAcrossClaimRules(t *testing.T) {
	residentID := integrityRunID(t, "01H00000000000000000000001")
	claimID := integrityRunID(t, "01H00000000000000000000021")
	sourceID := integrityRunID(t, "01H00000000000000000000031")
	inputs := []integrity.CandidateInput{
		{
			ResidentID: residentID, ClaimID: &claimID,
			Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimStatementErased,
			TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "statement_content_id",
			SourceContentErasureEventID: &sourceID, OccurredTZ: canonical.MustTimezone("UTC"),
		},
		{
			ResidentID: residentID, ClaimID: &claimID,
			Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimQualifyingSupportErased,
			TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "qualifying_support",
			SourceContentErasureEventID: &sourceID, OccurredTZ: canonical.MustTimezone("UTC"),
		},
	}
	fixture := newIntegrityRunFixture(t, inputs)
	candidates := make([]integrity.Candidate, 0, len(inputs))
	for _, input := range inputs {
		candidate, err := integrity.NewCandidate(input)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	planned, requiresMemoryStatus, err := fixture.service.plan(candidates)
	if err != nil {
		t.Fatal(err)
	}
	if !requiresMemoryStatus || len(planned) != 2 || planned[0].QuarantineTransitionID == nil ||
		planned[1].QuarantineTransitionID == nil ||
		*planned[0].QuarantineTransitionID != *planned[1].QuarantineTransitionID {
		t.Fatalf("same-claim quarantine plan = %+v, requires memory status=%v", planned, requiresMemoryStatus)
	}
	request := integrity.RecordFindings{
		ResidentID: residentID, PipelineVersionID: integrityRunID(t, "01H00000000000000000000041"),
		MemoryStatusPipelineVersionID: ptrIntegrityRunID(t, "01H00000000000000000000042"),
		Findings:                      planned,
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("shared transition request = %v", err)
	}
}

func ptrIntegrityRunID(t *testing.T, raw string) *canonical.ID {
	id := integrityRunID(t, raw)
	return &id
}

type integrityRunFixture struct {
	service *Service
	writer  *integrityRunWriter
}

func newIntegrityRunFixture(t *testing.T, inputs []integrity.CandidateInput) integrityRunFixture {
	t.Helper()
	repository := &integrityRunRepository{
		pipelines:       make(map[string]domain.PipelineVersionDefinition),
		pipelineCommits: make(map[canonical.ID]canonical.CommitMetadata),
		heads:           make(map[canonical.CommitSeq]integrity.HeadMetadata),
	}
	scanner := &integrityRunScanner{repository: repository, inputs: inputs}
	writer := &integrityRunWriter{
		t: t, repository: repository,
		findings:       make(map[canonical.Digest]canonical.ID),
		findingCommits: make(map[canonical.Digest]canonical.CommitMetadata),
		candidates:     make(map[canonical.ID][]integrity.Candidate),
	}
	for _, input := range inputs {
		candidate, err := integrity.NewCandidate(input)
		if err != nil {
			t.Fatal(err)
		}
		writer.candidates[candidate.ResidentID] = append(writer.candidates[candidate.ResidentID], candidate)
	}
	service, err := New(Options{
		Writer: writer, Repository: repository, Scanner: scanner,
		IDs: canonical.NewSecureIDGenerator(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return integrityRunFixture{service: service, writer: writer}
}

func integrityRunCandidate(t *testing.T, residentID canonical.ID, targetRaw string) integrity.CandidateInput {
	t.Helper()
	return integrity.CandidateInput{
		ResidentID:  residentID,
		Kind:        integrity.FindingProvenanceUnresolvable,
		RuleCode:    integrity.RuleGenerationInputSourceMissing,
		TargetKind:  integrity.TargetGenerationInput,
		TargetID:    integrityRunID(t, targetRaw),
		TargetField: "source",
		OccurredAt:  10,
		OccurredTZ:  canonical.MustTimezone("UTC"),
	}
}

type integrityRunRepository struct {
	pipelines       map[string]domain.PipelineVersionDefinition
	pipelineCommits map[canonical.ID]canonical.CommitMetadata
	head            integrity.HeadMetadata
	heads           map[canonical.CommitSeq]integrity.HeadMetadata
}

func (repository *integrityRunRepository) IntegrityPipelineCommit(
	_ context.Context,
	pipelineID canonical.ID,
) (canonical.CommitMetadata, error) {
	metadata, exists := repository.pipelineCommits[pipelineID]
	if !exists {
		return canonical.CommitMetadata{}, errors.New("pipeline commit not found")
	}
	return metadata, nil
}

func (repository *integrityRunRepository) PipelineVersion(
	_ context.Context,
	kind, version string,
) (domain.PipelineVersionDefinition, error) {
	definition, exists := repository.pipelines[kind+"\x00"+version]
	if !exists {
		return domain.PipelineVersionDefinition{}, fmt.Errorf("missing pipeline %s/%s", kind, version)
	}
	return definition, nil
}

func (repository *integrityRunRepository) IntegrityHeadMetadata(
	_ context.Context,
	head canonical.Head,
) (integrity.HeadMetadata, error) {
	if !head.Exists {
		if repository.head.Head.Exists {
			return integrity.HeadMetadata{}, errors.New("unexpected empty head")
		}
		return integrity.HeadMetadata{}, nil
	}
	metadata, exists := repository.heads[head.CommitSeq]
	if !exists || metadata.Head != head {
		return integrity.HeadMetadata{}, errors.New("head metadata not found")
	}
	return metadata, nil
}

type integrityRunScanner struct {
	repository *integrityRunRepository
	inputs     []integrity.CandidateInput
}

func (scanner *integrityRunScanner) Scan(context.Context) (integrity.ScanResult, error) {
	candidates := make([]integrity.Candidate, 0, len(scanner.inputs))
	for _, input := range scanner.inputs {
		candidate, err := integrity.NewCandidate(input)
		if err != nil {
			return integrity.ScanResult{}, err
		}
		candidates = append(candidates, candidate)
	}
	return integrity.ScanResult{CapturedHead: scanner.repository.head.Head, Candidates: candidates}, nil
}

type integrityRunWriter struct {
	t              *testing.T
	repository     *integrityRunRepository
	findings       map[canonical.Digest]canonical.ID
	findingCommits map[canonical.Digest]canonical.CommitMetadata
	candidates     map[canonical.ID][]integrity.Candidate
	residentScopes []canonical.ID
	nextCommit     int64
}

func (writer *integrityRunWriter) Submit(
	ctx context.Context,
	command canonical.Command,
) (canonical.CommandResult, error) {
	if err := ctx.Err(); err != nil {
		return canonical.CommandResult{}, err
	}
	if err := command.Validate(); err != nil {
		return canonical.CommandResult{}, err
	}
	switch command.Name() {
	case "RegisterPipelineVersions":
		if len(writer.repository.pipelines) != 0 {
			return canonical.CommandResult{}, nil
		}
		firstID, err := canonical.NewSecureIDGenerator().New()
		if err != nil {
			return canonical.CommandResult{}, err
		}
		secondID, err := canonical.NewSecureIDGenerator().New()
		if err != nil {
			return canonical.CommandResult{}, err
		}
		definitions, err := domain.IntegrityBootstrapPipelineDefinitions(firstID, secondID)
		if err != nil {
			return canonical.CommandResult{}, err
		}
		for _, definition := range definitions.Versions {
			writer.repository.pipelines[definition.Kind+"\x00"+definition.VersionKey] = definition
		}
		committed, err := writer.commit(command.Scope(), nil)
		if err != nil {
			return canonical.CommandResult{}, err
		}
		for _, definition := range definitions.Versions {
			writer.repository.pipelineCommits[definition.ID] = committed.Commit
		}
		return committed, nil
	case "RecordIntegrityFindings":
		residentID, scoped := command.Scope().ResidentID()
		if !scoped {
			return canonical.CommandResult{}, errors.New("record command is not resident scoped")
		}
		writer.residentScopes = append(writer.residentScopes, residentID)
		var result integrity.RecordFindingsResult
		for _, candidate := range writer.candidates[residentID] {
			if existingID, exists := writer.findings[candidate.Fingerprint]; exists {
				result.Existing = append(result.Existing, integrity.RecordedFinding{
					Fingerprint: candidate.Fingerprint, FindingID: existingID,
					Commit: writer.findingCommits[candidate.Fingerprint],
				})
				continue
			}
			findingID, err := canonical.NewSecureIDGenerator().New()
			if err != nil {
				return canonical.CommandResult{}, err
			}
			writer.findings[candidate.Fingerprint] = findingID
			result.Created = append(result.Created, integrity.RecordedFinding{
				Fingerprint: candidate.Fingerprint, FindingID: findingID, Created: true,
			})
			if candidate.RequiresQuarantine() {
				transitionID, err := canonical.NewSecureIDGenerator().New()
				if err != nil {
					return canonical.CommandResult{}, err
				}
				result.Quarantined = append(result.Quarantined, integrity.RecordedQuarantine{
					ClaimID: candidate.TargetID, FindingID: findingID, StatusTransitionID: transitionID,
				})
			}
		}
		if len(result.Created) == 0 {
			return canonical.CommandResult{Value: result}, nil
		}
		committed, err := writer.commit(command.Scope(), result)
		if err != nil {
			return canonical.CommandResult{}, err
		}
		for _, created := range result.Created {
			writer.findingCommits[created.Fingerprint] = committed.Commit
		}
		return committed, nil
	default:
		return canonical.CommandResult{}, fmt.Errorf("unexpected command %q", command.Name())
	}
}

func (writer *integrityRunWriter) commit(
	scope canonical.Scope,
	value any,
) (canonical.CommandResult, error) {
	writer.nextCommit++
	commitSeq, err := canonical.NewCommitSeq(writer.nextCommit)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	commitID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		return canonical.CommandResult{}, err
	}
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: commitSeq, Scope: scope,
		CommittedAt: canonical.Instant(writer.nextCommit), CommittedTZ: canonical.MustTimezone("UTC"),
	}
	head := canonical.Head{Exists: true, CommitSeq: commitSeq, CommittedAt: metadata.CommittedAt}
	rich := integrity.HeadMetadata{Head: head, CommitID: commitID, CommittedTZ: metadata.CommittedTZ}
	writer.repository.head = rich
	writer.repository.heads[commitSeq] = rich
	return canonical.CommandResult{Commit: metadata, Value: value}, nil
}

func integrityRunID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
