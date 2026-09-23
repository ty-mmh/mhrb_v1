package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/hostlock"
)

func TestCOV6MemoryPolicyActivateV4IsExplicitIdempotentAndOffline(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)

	runActivate := func(from string, acknowledgements bool) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		arguments := []string{
			"admin", "memory", "policy", "activate-v4", "--config", configPath,
			"--resident", residentID, "--from", from,
		}
		if acknowledgements {
			arguments = append(arguments, "--ack-enable-recall", "--ack-self-talk-extraction")
		}
		code := run(context.Background(), arguments, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runActivate("memory-policy-v1", true)
	if code != 0 {
		t.Fatalf("first activation exit=%d stderr=%s", code, stderr)
	}
	for _, field := range []string{
		`"changed": true`,
		`"previous_policy_version": "memory-policy-v1"`,
		`"policy_version": "memory-policy-v4"`,
		`"rendering_version": "memory-rendering-v2"`,
		`"definition_schema": "mahoroba-memory-policy-v0"`,
	} {
		if !strings.Contains(stdout, field) {
			t.Fatalf("activation output missing %s:\n%s", field, stdout)
		}
	}

	code, stdout, stderr = runActivate("memory-policy-v4", false)
	if code != 0 || !strings.Contains(stdout, `"changed": false`) {
		t.Fatalf("idempotent activation exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}

	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	code, _, stderr = runActivate("memory-policy-v4", false)
	if code != 1 || !strings.Contains(stderr, "already in use") {
		t.Fatalf("locked activation exit=%d stderr=%s", code, stderr)
	}
}

func TestCOV6LegacyPolicyCommandsRejectNewTransitionsAndV4ValidatesBeforeMutation(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	runCommand := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), arguments, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}
	code, _, stderr := runCommand(
		"admin", "memory", "policy", "activate-v0", "--config", configPath, "--resident", residentID,
	)
	if code != 1 || !strings.Contains(stderr, "use ActivateMemoryPolicyV4") {
		t.Fatalf("legacy v2 transition exit=%d stderr=%s", code, stderr)
	}
	code, _, stderr = runCommand(
		"admin", "memory", "policy", "activate-v4", "--config", configPath,
		"--resident", residentID, "--from", "memory-policy-v2",
		"--ack-enable-recall", "--ack-self-talk-extraction",
	)
	if code != 1 || !strings.Contains(stderr, "expected-from mismatch") {
		t.Fatalf("stale expected-from exit=%d stderr=%s", code, stderr)
	}
	verification, err := openRuntime(context.Background(), commandRuntimeConfig(dataDir), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close(commandRuntimeConfig(dataDir).Server.ShutdownTimeout)
	var versions int
	if err := verification.store.Reader().QueryRow(`SELECT COUNT(*) FROM pipeline_versions
		WHERE pipeline_kind IN ('memory_extraction','self_talk')`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 0 {
		t.Fatalf("rejected transitions registered %d pipelines", versions)
	}
}

func TestCOV6MemoryPolicyActivateV4RequiresExpectedFromBeforeOpeningRuntime(t *testing.T) {
	const residentID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"admin", "memory", "policy", "activate-v4", "--resident", residentID,
	}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "--from is required") {
		t.Fatalf("missing --from exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

type memoryCLIExtractionGenerator struct{}

func (memoryCLIExtractionGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	if request.Purpose == string(domain.GenerationPurposeMemoryExtraction) {
		return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"blue bicycle","statement":"The owner has a blue bicycle","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
	}
	return generation.Result{Text: "dialogue response"}, nil
}

type memoryCLIReplacementGenerator struct{ alignments int }

func (generator *memoryCLIReplacementGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	switch request.Purpose {
	case string(domain.GenerationPurposeMemoryExtraction):
		joined := ""
		for _, message := range request.Messages {
			joined += "\n" + message.Text
		}
		switch {
		case strings.Contains(joined, "I am composed"):
			return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am composed","statement":"The resident is composed.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
		case strings.Contains(joined, "I see calm"):
			return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
		default:
			return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
		}
	case string(domain.GenerationPurposeMemoryAlignment):
		generator.alignments++
		aligned := generator.alignments == 1
		return generation.Result{Text: map[bool]string{
			true:  `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`,
			false: `{"aligned":false,"confidence":"800000","version":"memory-alignment-output-v1"}`,
		}[aligned]}, nil
	default:
		return generation.Result{Text: "dialogue response"}, nil
	}
}

func TestMemoryClaimListAndShowUseQueryOnlyInspection(t *testing.T) {
	ctx := context.Background()
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)

	var stdout, stderr bytes.Buffer
	if code := run(ctx, []string{
		"admin", "memory", "policy", "activate-v4",
		"--config", configPath, "--resident", residentID, "--from", "memory-policy-v1",
		"--ack-enable-recall", "--ack-self-talk-extraction",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("activate exit=%d stderr=%s", code, stderr.String())
	}

	runtime, err := openRuntime(ctx, commandRuntimeConfig(dataDir), memoryCLIExtractionGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.app.Ingress(ctx, "I own a blue bicycle"); err != nil {
		t.Fatal(err)
	}
	resident, err := runtime.app.ActiveResident(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.ProcessResident(ctx, resident.ResidentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(commandRuntimeConfig(dataDir).Server.ShutdownTimeout); err != nil {
		t.Fatal(err)
	}
	watermarksBefore := projectionWatermarkCount(t, dataDir, residentID)

	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	stdout.Reset()
	stderr.Reset()
	if code := run(ctx, []string{
		"admin", "memory", "claim", "list", "--config", configPath,
		"--resident", residentID, "--scope", "resident_ui",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("claim list under host lock exit=%d stderr=%s", code, stderr.String())
	}
	var claims []domain.MemoryClaimSummary
	if err := json.Unmarshal(stdout.Bytes(), &claims); err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Statement != "[redacted]" {
		t.Fatalf("claim list = %+v", claims)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(ctx, []string{
		"admin", "memory", "claim", "show", "--config", configPath,
		"--resident", residentID, "--claim", claims[0].ClaimID.String(),
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("claim show under host lock exit=%d stderr=%s", code, stderr.String())
	}
	var provenance domain.MemoryClaimProvenance
	if err := json.Unmarshal(stdout.Bytes(), &provenance); err != nil {
		t.Fatal(err)
	}
	if len(provenance.Evidence) != 1 || provenance.Evidence[0].SourceEventContent != "I own a blue bicycle" {
		t.Fatalf("claim provenance = %+v", provenance)
	}
	if after := projectionWatermarkCount(t, dataDir, residentID); after != watermarksBefore {
		t.Fatalf("query-only Admin changed projection watermark count from %d to %d", watermarksBefore, after)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(ctx, []string{
		"admin", "memory", "persona", "list", "--config", configPath, "--resident", residentID,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("persona list under host lock exit=%d stderr=%s", code, stderr.String())
	}
	var revisions []domain.MemoryPersonaRevisionView
	if err := json.Unmarshal(stdout.Bytes(), &revisions); err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 1 || !revisions[0].Active || revisions[0].Content == "" {
		t.Fatalf("persona revisions = %+v", revisions)
	}
}

func TestMemoryGenerationAdminMutationsRequireOfflineHostLock(t *testing.T) {
	ctx := context.Background()
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	commands := [][]string{
		{"admin", "memory", "reextract", "--config", configPath, "--resident", residentID, "--event", residentID},
		{"admin", "memory", "abstract", "--config", configPath, "--resident", residentID, "--source", residentID, "--source", residentID},
		{"admin", "memory", "split", "--config", configPath, "--resident", residentID, "--source", residentID},
		{"admin", "memory", "persona", "propose", "--config", configPath, "--resident", residentID},
		{"admin", "memory", "claim", "status", "--config", configPath, "--resident", residentID,
			"--claim", residentID, "--to", "superseded", "--reason", "human_supersession", "--replacement", residentID},
	}
	for _, command := range commands {
		var stdout, stderr bytes.Buffer
		if code := run(ctx, command, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "already in use") {
			t.Fatalf("%v exit=%d stdout=%s stderr=%s", command, code, stdout.String(), stderr.String())
		}
	}
}

func TestMemoryReplacementIntentCLIRecordsCanonicalMarkerOffline(t *testing.T) {
	ctx := context.Background()
	dataDir := managedCommandDataDir(t)
	residentRaw := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	var stdout, stderr bytes.Buffer
	if code := run(ctx, []string{
		"admin", "memory", "policy", "activate-v4", "--config", configPath, "--resident", residentRaw,
		"--from", "memory-policy-v1", "--ack-enable-recall", "--ack-self-talk-extraction",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("activate exit=%d stderr=%s", code, stderr.String())
	}

	cfg := commandRuntimeConfig(dataDir)
	generator := &memoryCLIReplacementGenerator{}
	runtime, err := openRuntime(ctx, cfg, generator)
	if err != nil {
		t.Fatal(err)
	}
	for index, message := range []string{
		"I am calm", "I am calm", "I am calm", "I see calm", "I see calm",
		"I am composed", "I am composed", "I am composed",
	} {
		if _, err := runtime.app.Ingress(ctx, message); err != nil {
			t.Fatalf("ingress %d: %v", index, err)
		}
		resident, err := runtime.app.ActiveResident(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.app.ProcessResident(ctx, resident.ResidentID); err != nil {
			t.Fatalf("process %d: %v", index, err)
		}
		// A resident turn yields after its single fair mandatory-memory quantum.
		// Use a separate clean turn for alignment/maturation before the next
		// foreground message so this CLI fixture reaches the settled claim state
		// required by the replacement-intent contract.
		if err := runtime.app.ProcessResident(ctx, resident.ResidentID); err != nil {
			t.Fatalf("clean process %d: %v", index, err)
		}
	}
	// Foreground ingress invalidates the bounded normal/re-extraction discovery
	// proof.  The last extraction is durable after the loop above, but optional
	// alignment is admitted only after a fresh clean cycle has completed.  Give
	// that cycle a small explicit drain budget instead of depending on how many
	// discovery phases happen to fit in the final foreground turn.
	for pass := 0; pass < 4 && generator.alignments == 0; pass++ {
		resident, err := runtime.app.ActiveResident(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.app.ProcessResident(ctx, resident.ResidentID); err != nil {
			t.Fatalf("alignment drain %d: %v", pass, err)
		}
	}
	if generator.alignments == 0 {
		t.Fatal("bounded clean drain did not execute memory alignment")
	}
	claimID := func(statement string) string {
		t.Helper()
		var raw string
		if err := runtime.store.Reader().QueryRow(`SELECT claim.claim_id FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
			 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
			WHERE claim.owner_resident_id = ? AND blob.content = ?`, residentRaw, []byte(statement)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	oldClaimID := claimID("The resident is calm.")
	newClaimID := claimID("The resident is composed.")
	if err := runtime.Close(cfg.Server.ShutdownTimeout); err != nil {
		t.Fatal(err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(ctx, []string{
		"admin", "memory", "claim", "status", "--config", configPath, "--resident", residentRaw,
		"--claim", oldClaimID, "--to", "superseded", "--reason", "human_supersession",
		"--replacement", newClaimID,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("replacement intent exit=%d stderr=%s", code, stderr.String())
	}
	var result domain.MemoryReplacementIntentResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Created || result.NewClaimID.String() != newClaimID || result.OldClaimID.String() != oldClaimID {
		t.Fatalf("replacement CLI result = %+v", result)
	}

	verification, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close(cfg.Server.ShutdownTimeout)
	var relationType, reason string
	if err := verification.store.Reader().QueryRow(`SELECT relation_type, reason_code FROM claim_relations
		WHERE claim_relation_id = ? AND from_claim_id = ? AND to_claim_id = ?`,
		result.RelationID.String(), newClaimID, oldClaimID).Scan(&relationType, &reason); err != nil {
		t.Fatal(err)
	}
	if relationType != "contradicts" || reason != "replacement_intent" {
		t.Fatalf("replacement marker = %s/%s", relationType, reason)
	}
}

func TestMemoryReplacementIntentCLIRequiresSupersessionStatusContractBeforeOpeningRuntime(t *testing.T) {
	const validID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	var stdout, stderr bytes.Buffer
	command := []string{
		"admin", "memory", "claim", "status", "--resident", validID, "--claim", validID,
		"--to", "invalidated", "--reason", "human_invalidation", "--replacement", validID,
	}
	if code := run(context.Background(), command, &stdout, &stderr); code != 1 ||
		!strings.Contains(stderr.String(), "--replacement requires --to superseded --reason human_supersession") {
		t.Fatalf("replacement contract exit=%d stderr=%s", code, stderr.String())
	}
}

func TestMemoryDerivationCLIRejectsSourceCardinalityBeforeOpeningRuntime(t *testing.T) {
	const validID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, command := range [][]string{
		{"admin", "memory", "abstract", "--resident", validID, "--source", validID},
		{"admin", "memory", "split", "--resident", validID},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), command, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "requires") {
			t.Fatalf("%v exit=%d stderr=%s", command, code, stderr.String())
		}
	}
}
