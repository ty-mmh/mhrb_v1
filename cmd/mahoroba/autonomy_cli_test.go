package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/hostlock"
)

func TestAutonomyStatusPendingTriggerPrefersExecutableDurableWork(t *testing.T) {
	selfTalkSource, err := canonical.ParseID("01HF7YAT00A9954MJJA9954M80")
	if err != nil {
		t.Fatal(err)
	}
	initiativeSource, err := canonical.ParseID("01HF7YAT00A9954MJJA9954M81")
	if err != nil {
		t.Fatal(err)
	}
	durableSelfTalk := autonomy.Trigger{
		Kind: autonomy.TriggerIdle, SourceID: selfTalkSource, Ordinal: 2,
	}
	durableInitiative := autonomy.Trigger{
		Kind: autonomy.TriggerFutureToCurrent, SourceID: initiativeSource, Boundary: canonical.Instant(1),
	}
	selfTalk, initiative := executableAutonomyTriggers([]domain.AutonomousWork{
		{Purpose: domain.GenerationPurposeSelfTalk, Trigger: durableSelfTalk},
		{Purpose: domain.GenerationPurposeOutboundInitiative, Trigger: durableInitiative},
	})
	selfTalkIdentity, selfTalkErr := selfTalk.StableIdentity()
	wantSelfTalkIdentity, wantSelfTalkErr := durableSelfTalk.StableIdentity()
	initiativeIdentity, initiativeErr := initiative.StableIdentity()
	wantInitiativeIdentity, wantInitiativeErr := durableInitiative.StableIdentity()
	if selfTalkErr != nil || wantSelfTalkErr != nil || initiativeErr != nil || wantInitiativeErr != nil ||
		selfTalkIdentity != wantSelfTalkIdentity || initiativeIdentity != wantInitiativeIdentity {
		t.Fatalf("executable status triggers = %+v / %+v, want durable %+v / %+v",
			selfTalk, initiative, durableSelfTalk, durableInitiative)
	}
	discoveryCalls := 0
	resolved, err := resolveAutonomyStatusTrigger(selfTalk, func() ([]autonomy.Trigger, error) {
		discoveryCalls++
		return []autonomy.Trigger{{Kind: autonomy.TriggerIdle, SourceID: initiativeSource, Ordinal: 9}}, nil
	}, autonomy.Trigger{})
	if err != nil {
		t.Fatal(err)
	}
	resolvedIdentity, err := resolved.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if discoveryCalls != 0 || resolvedIdentity != wantSelfTalkIdentity {
		t.Fatalf("status durable precedence calls/trigger = %d/%s, want 0/%s",
			discoveryCalls, resolvedIdentity, wantSelfTalkIdentity)
	}
	if got := autonomyFeatureJSON(true, true, autonomy.Decision{}, selfTalk)["pending_trigger"]; got == nil {
		t.Fatal("durable self-talk work was not exposed as the pending trigger")
	}
	if got := autonomyFeatureJSON(true, true, autonomy.Decision{}, initiative)["pending_trigger"]; got == nil {
		t.Fatal("durable initiative work was not exposed as the pending trigger")
	}
}

func TestAutonomyMemoryPolicyActivationIsExplicitIdempotentAndOffline(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	runActivate := func(from string, acknowledgements bool) (int, string, string) {
		t.Helper()
		arguments := []string{
			"admin", "memory", "policy", "activate-v4",
			"--config", configPath, "--resident", residentID, "--from", from,
		}
		if acknowledgements {
			arguments = append(arguments, "--ack-enable-recall", "--ack-self-talk-extraction")
		}
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), arguments, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runActivate("memory-policy-v1", true)
	if code != 0 {
		t.Fatalf("first autonomy activation exit=%d stderr=%s", code, stderr)
	}
	for _, field := range []string{
		`"changed": true`, `"policy_version": "memory-policy-v4"`,
		`"definition_schema": "mahoroba-memory-policy-v0"`,
	} {
		if !strings.Contains(stdout, field) {
			t.Fatalf("autonomy activation output missing %s:\n%s", field, stdout)
		}
	}

	code, stdout, stderr = runActivate("memory-policy-v4", false)
	if code != 0 || !strings.Contains(stdout, `"changed": false`) {
		t.Fatalf("idempotent autonomy activation exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}

	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	code, _, stderr = runActivate("memory-policy-v4", false)
	if code != 1 || !strings.Contains(stderr, "already in use") {
		t.Fatalf("locked autonomy activation exit=%d stderr=%s", code, stderr)
	}
}

func TestAutonomyStatusAndRetentionCandidatesUseQueryOnlyInspection(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{
		"admin", "memory", "policy", "activate-v4",
		"--config", configPath, "--resident", residentID, "--from", "memory-policy-v1",
		"--ack-enable-recall", "--ack-self-talk-extraction",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("activate autonomy policy exit=%d stderr=%s", code, stderr.String())
	}
	watermarksBefore := projectionWatermarkCount(t, dataDir, residentID)

	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{
		"admin", "autonomy", "status", "--config", configPath, "--resident", residentID,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("autonomy status under host lock exit=%d stderr=%s", code, stderr.String())
	}
	for _, field := range []string{
		`"captured_head"`, `"active_memory_policy": "memory-policy-v4"`,
		`"active_autonomy_policy": "autonomy-policy-v0"`, `"counters"`,
		`"features"`, `"blocking_reason"`, `"pending_trigger"`,
	} {
		if !strings.Contains(stdout.String(), field) {
			t.Fatalf("autonomy status missing %s:\n%s", field, stdout.String())
		}
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{
		"admin", "autonomy", "retention", "candidates",
		"--config", configPath, "--resident", residentID,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("retention candidates under host lock exit=%d stderr=%s", code, stderr.String())
	}
	for _, field := range []string{`"captured_head"`, `"mode": "disabled"`, `"candidate_count": 0`, `"candidates": []`} {
		if !strings.Contains(stdout.String(), field) {
			t.Fatalf("retention output missing %s:\n%s", field, stdout.String())
		}
	}
	if after := projectionWatermarkCount(t, dataDir, residentID); after != watermarksBefore {
		t.Fatalf("query-only autonomy commands changed Projection watermarks from %d to %d", watermarksBefore, after)
	}
}
