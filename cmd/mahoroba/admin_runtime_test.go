package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/httpui"
)

func awaitManagedState(t *testing.T, controller *adminRuntimeController, state string) httpui.AdminRuntimeStatus {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		status, err := controller.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if status.State == state {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime state = %+v, want %s", status, state)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAdminRuntimeSerializesStartStopAndPreservesLifetime(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	readyGate := make(chan struct{})
	drainGate := make(chan struct{})
	var runs atomic.Int32
	controller := newAdminRuntimeController(parent, "http://configured/", make(chan struct{}, 1),
		func(ctx context.Context, ready func(string)) error {
			runs.Add(1)
			select {
			case <-readyGate:
				ready("http://127.0.0.1:12345/")
			case <-ctx.Done():
			}
			<-ctx.Done()
			<-drainGate
			return ctx.Err()
		})
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	status, err := controller.Start(requestCtx)
	if err != nil || status.State != "starting" || !status.Managed || status.Ready {
		t.Fatalf("start = %+v / %v", status, err)
	}
	cancelRequest()
	var callers sync.WaitGroup
	for range 8 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			_, _ = controller.Start(context.Background())
		}()
	}
	callers.Wait()
	close(readyGate)
	status = awaitManagedState(t, controller, "running")
	if runs.Load() != 1 || !status.Ready || status.URL != "http://127.0.0.1:12345/" {
		t.Fatalf("duplicate start or request cancellation affected runtime: runs=%d status=%+v", runs.Load(), status)
	}
	status, err = controller.Stop(context.Background())
	if err != nil || status.State != "stopping" || status.Ready {
		t.Fatalf("stop = %+v / %v", status, err)
	}
	if status, err = controller.Start(context.Background()); err == nil || status.State != "stopping" {
		t.Fatalf("start crossed an unfinished drain: %+v / %v", status, err)
	}
	close(drainGate)
	status = awaitManagedState(t, controller, "stopped")
	if status.Managed || status.Error != "" || status.RestartRequired {
		t.Fatalf("clean stop reported failure: %+v", status)
	}
	if err := controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Start(context.Background()); err == nil {
		t.Fatal("start accepted after administration closed")
	}
}

func TestAdminRuntimeStopDuringStartupDoesNotBecomeRunning(t *testing.T) {
	stoppingObserved := make(chan struct{})
	controller := newAdminRuntimeController(context.Background(), "", make(chan struct{}, 1),
		func(ctx context.Context, ready func(string)) error {
			<-ctx.Done()
			ready("http://should-not-become-ready/")
			close(stoppingObserved)
			return fmt.Errorf("startup canceled: %w", ctx.Err())
		})
	_, _ = controller.Start(context.Background())
	_, _ = controller.Stop(context.Background())
	<-stoppingObserved
	status := awaitManagedState(t, controller, "stopped")
	if status.Ready || status.Managed {
		t.Fatalf("canceled startup reported ready: %+v", status)
	}
}

func TestAdminRuntimeFailureRetryAndIncompleteCleanup(t *testing.T) {
	var runs atomic.Int32
	controller := newAdminRuntimeController(context.Background(), "", make(chan struct{}, 1),
		func(ctx context.Context, ready func(string)) error {
			if runs.Add(1) == 1 {
				return errors.New("runtime readiness: memory_policy_not_service_current")
			}
			ready("http://127.0.0.1:12345/")
			<-ctx.Done()
			return errors.Join(context.Canceled, errServeCleanupIncomplete, errors.New("workers did not drain"))
		})
	_, _ = controller.Start(context.Background())
	status := awaitManagedState(t, controller, "failed")
	if !strings.Contains(status.Error, "memory_policy_not_service_current") || status.RestartRequired || status.Managed {
		t.Fatalf("preflight failure = %+v", status)
	}
	if _, err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitManagedState(t, controller, "running")
	_, _ = controller.Stop(context.Background())
	status = awaitManagedState(t, controller, "failed")
	if !status.RestartRequired || !status.Managed || !strings.Contains(status.Error, "workers did not drain") {
		t.Fatalf("unsafe cleanup reported stopped: %+v", status)
	}
	if _, err := controller.Start(context.Background()); !errors.Is(err, errServeCleanupIncomplete) {
		t.Fatalf("restart accepted after incomplete cleanup: %v", err)
	}
	if err := controller.Close(context.Background()); !errors.Is(err, errServeCleanupIncomplete) {
		t.Fatalf("close lost cleanup failure: %v", err)
	}
}

func TestAdminRuntimeCloseCancelsAndJoins(t *testing.T) {
	drain := make(chan struct{})
	controller := newAdminRuntimeController(context.Background(), "", make(chan struct{}, 1),
		func(ctx context.Context, ready func(string)) error {
			ready("http://127.0.0.1:12345/")
			<-ctx.Done()
			<-drain
			return nil
		})
	_, _ = controller.Start(context.Background())
	awaitManagedState(t, controller, "running")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not join pending drain: %v", err)
	}
	close(drain)
	if err := controller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := awaitManagedState(t, controller, "stopped")
	if status.Managed {
		t.Fatalf("closed controller still owns runtime: %+v", status)
	}
}

func TestAdminRuntimeCoordinatesOfflineCommandsAndIgnoresExternalRuntime(t *testing.T) {
	service := newAdminCLIService("", "unused")
	service.runtime = newAdminRuntimeController(context.Background(), "", service.admission,
		func(ctx context.Context, ready func(string)) error {
			ready("http://127.0.0.1:12345/")
			<-ctx.Done()
			return nil
		})
	// An occupied management token represents an in-progress finite CLI command.
	service.admission <- struct{}{}
	if _, err := service.StartRuntime(context.Background()); err == nil {
		t.Fatal("runtime start raced a finite operation")
	}
	<-service.admission
	_, _ = service.StartRuntime(context.Background())
	awaitManagedState(t, service.runtime, "running")
	result, err := service.Execute(context.Background(), "admin.resident.list", nil)
	if err != nil || result.ExitCode == 0 || !strings.Contains(result.Stderr, "stop the managed dialogue server") {
		t.Fatalf("offline operation crossed owned runtime: %+v / %v", result, err)
	}
	if err := service.runtime.AllowCommand(false); err != nil {
		t.Fatalf("query-only operation incorrectly blocked: %v", err)
	}
	_, _ = service.StopRuntime(context.Background())
	awaitManagedState(t, service.runtime, "stopped")
	if err := service.runtime.AllowCommand(true); err != nil {
		t.Fatal(err)
	}

	// No owned run exists, so Stop must not touch a separately held host lock.
	dataDir := managedCommandDataDir(t)
	external, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	_, _ = service.StopRuntime(context.Background())
	if competing, err := hostlock.Acquire(dataDir); err == nil {
		_ = competing.Close()
		t.Fatal("stop released a separately owned runtime lock")
	}
}

func TestAdminRuntimeRealServeStartStopRestartAndReadinessFailure(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	for _, name := range []string{"MAHOROBA_PROVIDER_API_KEY", "MAHOROBA_PROVIDER_API_KEY_FILE", "MAHOROBA_TTS_API_KEY", "MAHOROBA_TTS_API_KEY_FILE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("MAHOROBA_PROVIDER_API_KEY", "test-key")
	service := newAdminCLIService("", dataDir)
	initResult, err := service.Execute(context.Background(), "admin.bootstrap.init", nil)
	if err != nil || initResult.ExitCode != 0 {
		t.Fatalf("bootstrap init: %+v / %v", initResult, err)
	}
	// Obtain the new resident through the application's existing JSON output.
	residentID := adminTestResidentID(t, initResult.Stdout)
	for _, command := range []string{"admin.bootstrap.approve", "admin.bootstrap.finalize"} {
		result, err := service.Execute(context.Background(), command, map[string][]string{"resident": {residentID}})
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("%s: %+v / %v", command, result, err)
		}
	}
	configPath := filepath.Join(t.TempDir(), "runtime.toml")
	configText := fmt.Sprintf("data_dir = %q\n[server]\nlisten = \"127.0.0.1:0\"\nshutdown_timeout = \"2s\"\n[generation]\nbase_url = \"http://127.0.0.1:1/v1\"\nmodel = \"test-model\"\n", dataDir)
	if err := os.WriteFile(configPath, []byte(configText), 0600); err != nil {
		t.Fatal(err)
	}
	controller := newAdminRuntimeController(context.Background(), "", service.admission,
		func(ctx context.Context, ready func(string)) error {
			return runServeWithLifecycle(ctx, []string{"--config", configPath, "--data-dir", dataDir}, io.Discard, io.Discard,
				serveLifecycleHooks{Ready: ready, ManagementURL: "http://127.0.0.1:8788/"})
		})
	service.runtime = controller
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := controller.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	for attempt := range 2 {
		if _, err := controller.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		status := awaitManagedState(t, controller, "running")
		client := &http.Client{Timeout: time.Second}
		response, err := client.Get(status.URL + "healthz")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("ready callback preceded service readiness: HTTP %d %s", response.StatusCode, body)
		}
		_, _ = controller.Stop(context.Background())
		status = awaitManagedState(t, controller, "stopped")
		lock, err := hostlock.Acquire(dataDir)
		if err != nil {
			t.Fatalf("attempt %d retained host lock: %v / %+v", attempt, err, status)
		}
		_ = lock.Close()
		address := strings.TrimSuffix(strings.TrimPrefix(status.URL, "http://"), "/")
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("attempt %d retained listener: %v", attempt, err)
		}
		_ = listener.Close()
	}
	// Archive the selected resident in this isolated fixture. Managed start
	// must preserve the ordinary resident-readiness rejection.
	result, err := service.Execute(context.Background(), "admin.resident.archive", map[string][]string{"resident": {residentID}})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("archive isolated resident: %+v / %v", result, err)
	}
	_, _ = controller.Start(context.Background())
	status := awaitManagedState(t, controller, "failed")
	if !strings.Contains(status.Error, "runtime readiness: service not ready: active_resident_") || status.RestartRequired {
		t.Fatalf("readiness boundary changed: %+v", status)
	}
}

func adminTestResidentID(t *testing.T, output string) string {
	t.Helper()
	var result struct {
		Residents []struct {
			ID string `json:"resident_id"`
		} `json:"residents"`
	}
	if err := json.Unmarshal([]byte(output), &result); err != nil || len(result.Residents) != 1 {
		t.Fatalf("resident bootstrap output = %s / %v", output, err)
	}
	return result.Residents[0].ID
}
