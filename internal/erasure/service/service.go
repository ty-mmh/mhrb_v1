// Package service owns the offline public erasure plan/decision artifact
// boundary. Canonical mutation remains in erasure.ApplyCommand and the SQLite
// UoW; this package only captures direct-read plans and durably publishes
// their exact JCS bytes.
package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/readiness"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

const maxPlanBytes = 16 << 20

var (
	ErrSourceUnavailable    = errors.New("erasure service: source unavailable")
	ErrArtifactTargetExists = errors.New("erasure service: artifact target exists")
	ErrArtifactIO           = errors.New("erasure service: artifact I/O failed")
	ErrDurabilityUnknown    = errors.New("erasure service: publish durability unknown")
)

type PlanRequest struct {
	SourceDataDir    string
	DatabaseFilename string
	Scope            string
	ResidentID       canonical.ID
	RequestedContent []canonical.ID
	ReasonCode       string
	Output           string
}

type DecideRequest struct {
	SourceDataDir    string
	DatabaseFilename string
	InputPlan        string
	Decisions        []erasure.DecisionInput
	Output           string
}

type ArtifactResult struct {
	ArtifactPath string
	Plan         erasure.Plan
	BaseHead     readiness.Head
}

type sourceBoundary struct {
	dataDir    string
	identity   string
	lock       *hostlock.Lock
	database   *storesqlite.BoundDatabase
	inspection *storesqlite.Inspection
	blobs      *blob.FileStore
}

// publicationCloseTestHook is nil in production. Tests use it to prove that
// a post-barrier handle/lock cleanup failure cannot turn a durably visible
// artifact into a false unpublished result.
var publicationCloseTestHook func() error

// sourceBoundaryTestHook is nil in production. Tests use the named points to
// force a source identity change around publication.
var sourceBoundaryTestHook func(string)

func (boundary *sourceBoundary) close() error {
	if boundary == nil {
		return nil
	}
	var databaseErr, lockErr error
	if boundary.database != nil {
		databaseErr = boundary.database.Close()
	}
	if boundary.lock != nil {
		lockErr = boundary.lock.Close()
	}
	return errors.Join(databaseErr, lockErr)
}

func (boundary *sourceBoundary) verify() error {
	if boundary == nil || boundary.database == nil {
		return fmt.Errorf("%w: incomplete source boundary", ErrSourceUnavailable)
	}
	if err := boundary.database.Verify(); err != nil {
		return fmt.Errorf("%w: source database identity changed: %v", ErrSourceUnavailable, err)
	}
	return nil
}

func runSourceBoundaryTestHook(stage string) {
	if sourceBoundaryTestHook != nil {
		sourceBoundaryTestHook(stage)
	}
}

func CreatePlan(ctx context.Context, request PlanRequest) (_ ArtifactResult, resultErr error) {
	if ctx == nil {
		return ArtifactResult{}, fmt.Errorf("%w: nil context", ErrSourceUnavailable)
	}
	if request.Scope != erasure.ScopeContent && request.Scope != erasure.ScopeResident {
		return ArtifactResult{}, fmt.Errorf("%w: invalid scope", erasure.ErrInvalidPlan)
	}
	if request.Scope == erasure.ScopeContent && len(request.RequestedContent) == 0 ||
		request.Scope == erasure.ScopeResident && len(request.RequestedContent) != 0 {
		return ArtifactResult{}, fmt.Errorf("%w: invalid requested content shape", erasure.ErrInvalidPlan)
	}
	output, err := preflightOutput(request.SourceDataDir, request.Output)
	if err != nil {
		return ArtifactResult{}, err
	}
	boundary, err := openSource(ctx, request.SourceDataDir, request.DatabaseFilename)
	if err != nil {
		return ArtifactResult{}, err
	}
	published := false
	defer func() {
		closeErr := boundary.close()
		if !published {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	authority, err := boundary.inspection.ErasurePlanningAuthority(ctx, request.ResidentID)
	if err != nil {
		return ArtifactResult{}, err
	}
	planner := erasure.Planner{
		Source: boundary.inspection.ErasureSource(boundary.blobs),
		IDs:    canonical.NewSecureIDGenerator(),
	}
	plan, err := planner.Plan(ctx, erasure.PlanRequest{
		Scope: request.Scope, ResidentID: request.ResidentID,
		ActorPrincipalID: authority.ActorPrincipalID, ReasonCode: request.ReasonCode,
		IntegrityPipelineVersionID:    authority.IntegrityPipelineVersionID,
		MemoryStatusPipelineVersionID: authority.MemoryStatusPipelineVersionID,
		RequestedContentIDs:           request.RequestedContent,
	})
	if err != nil {
		return ArtifactResult{}, err
	}
	head, err := captureMatchingHead(ctx, boundary.inspection, plan.BaseHead)
	if err != nil {
		return ArtifactResult{}, err
	}
	body, err := erasure.MarshalPlan(plan)
	if err != nil {
		return ArtifactResult{}, err
	}
	inputDigest, err := planProducerInputDigest(boundary.identity, plan)
	if err != nil {
		return ArtifactResult{}, fmt.Errorf("%w: bind producer input", ErrArtifactIO)
	}
	command := durablepublish.CommandErasurePlanResident
	if request.Scope == erasure.ScopeContent {
		command = durablepublish.CommandErasurePlanContent
	}
	runSourceBoundaryTestHook("before_publish")
	if err := boundary.verify(); err != nil {
		return ArtifactResult{}, err
	}
	if err := publishPlan(ctx, output, command, inputDigest, body); err != nil {
		return ArtifactResult{}, err
	}
	published = true
	runSourceBoundaryTestHook("after_publish")
	if err := boundary.verify(); err != nil {
		return ArtifactResult{}, fmt.Errorf("%w: source boundary changed after publication: %v", ErrDurabilityUnknown, err)
	}
	return ArtifactResult{ArtifactPath: output, Plan: plan, BaseHead: head}, nil
}

func Decide(ctx context.Context, request DecideRequest) (_ ArtifactResult, resultErr error) {
	if ctx == nil {
		return ArtifactResult{}, fmt.Errorf("%w: nil context", ErrSourceUnavailable)
	}
	output, err := preflightOutput(request.SourceDataDir, request.Output)
	if err != nil {
		return ArtifactResult{}, err
	}
	boundary, err := openSource(ctx, request.SourceDataDir, request.DatabaseFilename)
	if err != nil {
		return ArtifactResult{}, err
	}
	published := false
	defer func() {
		closeErr := boundary.close()
		if !published {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	inputPlan, inputIdentity, err := ReadPlan(ctx, request.InputPlan)
	if err != nil {
		return ArtifactResult{}, err
	}
	decisions := slices.Clone(request.Decisions)
	slices.SortFunc(decisions, func(left, right erasure.DecisionInput) int {
		return strings.Compare(left.ImpactID, right.ImpactID)
	})
	planner := erasure.Planner{
		Source: boundary.inspection.ErasureSource(boundary.blobs),
		IDs:    canonical.NewSecureIDGenerator(),
	}
	plan, err := planner.Decide(ctx, inputPlan, decisions)
	if err != nil {
		return ArtifactResult{}, err
	}
	head, err := captureMatchingHead(ctx, boundary.inspection, plan.BaseHead)
	if err != nil {
		return ArtifactResult{}, err
	}
	body, err := erasure.MarshalPlan(plan)
	if err != nil {
		return ArtifactResult{}, err
	}
	producerDecisions := make([]durablepublish.ErasureDecision, 0, len(decisions))
	for _, decision := range decisions {
		producerDecisions = append(producerDecisions, durablepublish.ErasureDecision{
			ImpactID: decision.ImpactID, Decision: decision.Decision,
		})
	}
	inputDigest, err := durablepublish.ProducerInputDigest(
		durablepublish.CommandErasureDecide,
		durablepublish.ErasureDecideProducerInput{
			SourceDataDirIdentity: boundary.identity,
			InputPlanIdentity:     inputIdentity, InputPlanDigest: inputPlan.Digest,
			Decisions: producerDecisions,
		},
	)
	if err != nil {
		return ArtifactResult{}, fmt.Errorf("%w: bind decision input", ErrArtifactIO)
	}
	runSourceBoundaryTestHook("before_publish")
	if err := boundary.verify(); err != nil {
		return ArtifactResult{}, err
	}
	if err := publishPlan(ctx, output, durablepublish.CommandErasureDecide, inputDigest, body); err != nil {
		return ArtifactResult{}, err
	}
	published = true
	runSourceBoundaryTestHook("after_publish")
	if err := boundary.verify(); err != nil {
		return ArtifactResult{}, fmt.Errorf("%w: source boundary changed after publication: %v", ErrDurabilityUnknown, err)
	}
	return ArtifactResult{ArtifactPath: output, Plan: plan, BaseHead: head}, nil
}

func openSource(ctx context.Context, dataDir, databaseFilename string) (*sourceBoundary, error) {
	if dataDir == "" || !filepath.IsAbs(dataDir) || databaseFilename == "" ||
		filepath.Base(databaseFilename) != databaseFilename || strings.ContainsAny(databaseFilename, `/\\`) {
		return nil, fmt.Errorf("%w: invalid source", ErrSourceUnavailable)
	}
	canonicalDataDir, err := canonicalDirectory(dataDir)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve source", ErrSourceUnavailable)
	}
	lock, err := hostlock.Acquire(canonicalDataDir)
	if err != nil {
		return nil, err
	}
	boundary := &sourceBoundary{dataDir: canonicalDataDir, lock: lock}
	fail := func(cause error) (*sourceBoundary, error) {
		return nil, errors.Join(cause, boundary.close())
	}
	boundary.database, err = storesqlite.OpenReadOnlyBoundDatabase(ctx, canonicalDataDir, databaseFilename)
	if err != nil {
		return fail(fmt.Errorf("%w: open bound source database: %v", ErrSourceUnavailable, err))
	}
	boundary.inspection = boundary.database.Inspection()
	boundary.identity, err = boundary.database.SourceIdentityDigest()
	if err != nil {
		return fail(fmt.Errorf("%w: bind source root", ErrSourceUnavailable))
	}
	if err := boundary.verify(); err != nil {
		return fail(err)
	}
	boundary.blobs, err = blob.OpenFileStoreReadOnly(filepath.Join(canonicalDataDir, "blobs"))
	if err != nil {
		return fail(fmt.Errorf("%w: open source blobs", ErrSourceUnavailable))
	}
	if err := boundary.inspection.MinimumCheckerWithBlobObjects(boundary.blobs).Check(ctx); err != nil {
		return fail(err)
	}
	if err := boundary.verify(); err != nil {
		return fail(err)
	}
	return boundary, nil
}

func captureMatchingHead(ctx context.Context, inspection *storesqlite.Inspection, base erasure.BaseHead) (readiness.Head, error) {
	snapshot, err := inspection.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		return readiness.Head{}, err
	}
	if !snapshot.CapturedHead.Exists || snapshot.CapturedHead.CommitID.String() != base.CommitID ||
		snapshot.CapturedHead.CommitSeq.String() != base.CommitSeq {
		return readiness.Head{}, erasure.ErrPlanStale
	}
	return snapshot.CapturedHead, nil
}

func planProducerInputDigest(sourceIdentity string, plan erasure.Plan) (canonical.Digest, error) {
	base := durablepublish.ErasurePlanBaseHead{CommitID: plan.BaseHead.CommitID, CommitSeq: plan.BaseHead.CommitSeq}
	if plan.Scope == erasure.ScopeContent {
		return durablepublish.ProducerInputDigest(
			durablepublish.CommandErasurePlanContent,
			durablepublish.ErasurePlanContentProducerInput{
				SourceDataDirIdentity: sourceIdentity, ResidentID: plan.ResidentID,
				RequestedContentIDs: slices.Clone(plan.RequestedContentIDs), ReasonCode: plan.ReasonCode,
				BaseHead: base, ImpactVersion: plan.RulesVersion,
			},
		)
	}
	return durablepublish.ProducerInputDigest(
		durablepublish.CommandErasurePlanResident,
		durablepublish.ErasurePlanResidentProducerInput{
			SourceDataDirIdentity: sourceIdentity, ResidentID: plan.ResidentID,
			ReasonCode: plan.ReasonCode, BaseHead: base, ImpactVersion: plan.RulesVersion,
		},
	)
}

// ReadPlan is the official consumer protocol for a single-file erasure
// artifact. It holds the exact target namespace lock, rejects a matching
// publish-pending sibling, and parses bytes from one retained no-follow handle.
func ReadPlan(ctx context.Context, input string) (_ erasure.Plan, identity string, resultErr error) {
	if ctx == nil || input == "" || !filepath.IsAbs(input) {
		return erasure.Plan{}, "", fmt.Errorf("%w: invalid plan path", ErrSourceUnavailable)
	}
	parentPath, err := canonicalDirectory(filepath.Dir(filepath.Clean(input)))
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: resolve plan parent", ErrSourceUnavailable)
	}
	input = filepath.Join(parentPath, filepath.Base(filepath.Clean(input)))
	lock, err := namespacelock.AcquireExistingParent(input)
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: lock plan namespace", ErrSourceUnavailable)
	}
	completed := false
	defer func() {
		closeErr := lock.Close()
		if !completed {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: filesystem policy", ErrSourceUnavailable)
	}
	// The namespace rendezvous retains a write/delete-sharing-denying parent
	// handle on Windows. Reopen the managed parent with the compatible
	// handle-relative profile, then grant only OpenRegularRead below.
	parent, err := fssecure.OpenRoot(parentPath, policy)
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: open plan parent: %v", ErrSourceUnavailable, err)
	}
	defer func() {
		closeErr := parent.Close()
		if !completed {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if err := rejectPendingMarker(parent, filepath.Base(input)); err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: plan publication is incomplete", ErrSourceUnavailable)
	}
	handle, err := parent.OpenRegularRead(filepath.Base(input))
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: open plan", ErrSourceUnavailable)
	}
	defer func() {
		closeErr := handle.Close()
		if !completed {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	body, err := io.ReadAll(io.LimitReader(handle.File(), maxPlanBytes+1))
	if err != nil || len(body) > maxPlanBytes {
		return erasure.Plan{}, "", fmt.Errorf("%w: read bounded plan", ErrSourceUnavailable)
	}
	identity, err = handle.ArtifactSourceIdentityDigest()
	if err != nil {
		return erasure.Plan{}, "", fmt.Errorf("%w: bind plan identity", ErrSourceUnavailable)
	}
	plan, err := erasure.ParsePlan(body)
	if err != nil {
		return erasure.Plan{}, "", err
	}
	completed = true
	return plan, identity, nil
}

func publishPlan(ctx context.Context, output, command string, inputDigest canonical.Digest, body []byte) (resultErr error) {
	lock, err := namespacelock.AcquireExistingParent(output)
	if err != nil {
		if errors.Is(err, namespacelock.ErrBusy) {
			return fmt.Errorf("%w: output namespace busy", ErrArtifactIO)
		}
		return errors.Join(ErrArtifactIO, fmt.Errorf("lock output namespace: %w", err))
	}
	published := false
	defer func() {
		closeErr := lock.Close()
		if publicationCloseTestHook != nil {
			closeErr = errors.Join(closeErr, publicationCloseTestHook())
		}
		if !published {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return fmt.Errorf("%w: filesystem policy", ErrArtifactIO)
	}
	parent, err := fssecure.OpenRoot(filepath.Dir(output), policy)
	if err != nil {
		return fmt.Errorf("%w: open output parent", ErrArtifactIO)
	}
	defer func() {
		closeErr := parent.Close()
		if !published {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if err := rejectPendingMarker(parent, filepath.Base(output)); err != nil {
		return err
	}
	exists, err := lock.RegularTargetExists()
	if err != nil {
		return fmt.Errorf("%w: inspect output target", ErrArtifactIO)
	}
	if exists {
		return ErrArtifactTargetExists
	}
	generator, err := canonical.NewIDGenerator(canonical.SystemClock{}, rand.Reader)
	if err != nil {
		return fmt.Errorf("%w: allocate publication identity", ErrArtifactIO)
	}
	publishID, err := generator.New()
	if err != nil {
		return fmt.Errorf("%w: allocate publication identity", ErrArtifactIO)
	}
	targetName := filepath.Base(output)
	stagingName := "." + targetName + ".staging." + publishID.String()
	staging, err := parent.CreateRegular(stagingName)
	if err != nil {
		return fmt.Errorf("%w: create staging plan", ErrArtifactIO)
	}
	publicationStarted := false
	defer func() {
		var closeErr error
		if !publicationStarted {
			closeErr = staging.MarkDeleteOnClose()
		}
		closeErr = errors.Join(closeErr, staging.Close())
		if !published {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if _, err := staging.File().Write(body); err != nil {
		return fmt.Errorf("%w: write staging plan", ErrArtifactIO)
	}
	if err := staging.Seal(); err != nil {
		return fmt.Errorf("%w: seal staging plan", ErrArtifactIO)
	}
	payload, err := durablepublish.SingleFilePayloadDigest(ctx, staging)
	if err != nil {
		return fmt.Errorf("%w: hash staging plan", ErrArtifactIO)
	}
	marker, err := durablepublish.NewMarker(
		publishID, durablepublish.VariantSingleFile, command, inputDigest,
		targetName, stagingName, payload.SHA256,
	)
	if err != nil {
		return fmt.Errorf("%w: construct publication marker", ErrArtifactIO)
	}
	publicationStarted = true
	if _, err := durablepublish.PublishSingleFile(ctx, durablepublish.SingleFileRequest{
		Parent: parent, Staging: staging, Marker: marker,
	}); err != nil {
		siblingName, nameErr := durablepublish.SiblingMarkerBasename(targetName, publishID)
		if nameErr != nil {
			return fmt.Errorf("%w: publication marker identity", ErrDurabilityUnknown)
		}
		sibling, siblingErr := parent.OpenRegularRead(siblingName)
		if siblingErr == nil {
			_ = sibling.Close()
			return fmt.Errorf("%w: plan publication requires recovery", ErrDurabilityUnknown)
		}
		if !errors.Is(siblingErr, os.ErrNotExist) {
			return fmt.Errorf("%w: publication marker state is unknown", ErrDurabilityUnknown)
		}
		publicationStarted = false
		if errors.Is(err, durablepublish.ErrTargetExists) {
			return ErrArtifactTargetExists
		}
		return fmt.Errorf("%w: publish staging plan", ErrArtifactIO)
	}
	published = true
	return nil
}

func rejectPendingMarker(parent *fssecure.Directory, target string) error {
	entries, err := parent.ReadDir()
	if err != nil {
		return fmt.Errorf("%w: inspect publication marker", ErrArtifactIO)
	}
	prefix := "." + target + ".publish-pending."
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			return fmt.Errorf("%w: plan has pending publication marker", ErrDurabilityUnknown)
		}
	}
	return nil
}

func preflightOutput(sourceDataDir, output string) (string, error) {
	if output == "" || !filepath.IsAbs(output) {
		return "", fmt.Errorf("%w: output must be absolute", ErrArtifactIO)
	}
	clean := filepath.Clean(output)
	if base := filepath.Base(clean); base == "" || base == "." || strings.ContainsAny(base, `/\\`) {
		return "", fmt.Errorf("%w: output basename", ErrArtifactIO)
	}
	parent, err := canonicalDirectory(filepath.Dir(clean))
	if err != nil {
		return "", fmt.Errorf("%w: output parent", ErrArtifactIO)
	}
	output = filepath.Join(parent, filepath.Base(clean))
	if sourceDataDir != "" {
		if source, err := canonicalDirectory(sourceDataDir); err == nil && pathWithin(source, output) {
			return "", fmt.Errorf("%w: output overlaps source", ErrArtifactIO)
		}
	}
	return output, nil
}

func canonicalDirectory(value string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(value))
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("not a stable directory")
	}
	return filepath.Clean(resolved), nil
}

func pathWithin(parent, child string) bool {
	if runtime.GOOS == "windows" {
		parent, child = strings.ToLower(parent), strings.ToLower(child)
	}
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}
