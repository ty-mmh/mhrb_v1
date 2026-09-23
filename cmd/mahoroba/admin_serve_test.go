package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/httpui"
)

func TestAdminArgumentsKeepInputInsideAllowlistedFlags(t *testing.T) {
	service := newAdminCLIService("fixed-config.toml", "fixed-data")
	attack := "--data-dir=elsewhere; echo injected"
	argv, err := service.arguments("admin.bootstrap.init", map[string][]string{"name": {attack}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, arg := range argv {
		found = found || arg == "--name="+attack
		if arg == "--data-dir=elsewhere" {
			t.Fatal("input introduced a source override")
		}
	}
	if !found || argv[len(argv)-2] != "--config=fixed-config.toml" || argv[len(argv)-1] != "--data-dir=fixed-data" {
		t.Fatalf("unexpected argv: %#v", argv)
	}

	for _, testCase := range []struct {
		name, command string
		values        map[string][]string
	}{
		{"unknown command", "admin.serve", nil},
		{"shell command", "powershell", nil},
		{"source override", "db.verify", map[string][]string{"data-dir": {"other"}}},
		{"config override", "db.verify", map[string][]string{"config": {"other"}}},
		{"unknown field", "db.verify", map[string][]string{"output": {"other"}}},
		{"duplicate field", "admin.bootstrap.init", map[string][]string{"name": {"first", "second"}}},
		{"boolean", "admin.bootstrap.finalize", map[string][]string{"resident": {"id"}, "select": {"yes"}}},
		{"choice", "admin.memory.claim.scope", map[string][]string{"resident": {"id"}, "claim": {"id"}, "scope": {"public"}}},
		{"missing required", "admin.memory.claim.show", nil},
		{"nul", "admin.bootstrap.init", map[string][]string{"name": {"bad\x00name"}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := service.arguments(testCase.command, testCase.values); err == nil {
				t.Fatal("unsafe or invalid request was accepted")
			}
		})
	}
}

func TestAdminArgumentsPreserveRepeatableAndExplicitFalseValues(t *testing.T) {
	service := newAdminCLIService("", "fixed-data")
	argv, err := service.arguments("admin.memory.abstract", map[string][]string{
		"resident": {"resident"}, "source": {"first", "second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"admin", "memory", "abstract", "--resident=resident", "--source=first", "--source=second", "--data-dir=fixed-data"}
	if !reflect.DeepEqual(argv, want) {
		t.Fatalf("argv = %#v, want %#v", argv, want)
	}
	argv, err = service.arguments("admin.bootstrap.finalize", map[string][]string{"resident": {"resident"}, "select": {"false"}})
	if err != nil || !strings.Contains(strings.Join(argv, "\n"), "--select=false") {
		t.Fatalf("unchecked default-true option was lost: %#v / %v", argv, err)
	}
}

func TestAdminArgumentsRespectCommandSpecificSourceFlags(t *testing.T) {
	service := newAdminCLIService("fixed.toml", "fixed-data")
	for _, testCase := range []struct {
		command         string
		values          map[string][]string
		config, dataDir bool
	}{
		{"backup.verify", map[string][]string{"input": {"bundle"}}, false, false},
		{"backup.restore", map[string][]string{"input": {"bundle"}, "target-data-dir": {"new-data"}}, true, false},
		{"healthcheck", nil, true, false},
		{"db.verify", nil, true, true},
	} {
		argv, err := service.arguments(testCase.command, testCase.values)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(argv, "\n")
		if strings.Contains(joined, "--config=") != testCase.config || strings.Contains(joined, "\n--data-dir=") != testCase.dataDir {
			t.Fatalf("%s source arguments = %#v", testCase.command, argv)
		}
	}
}

func TestAdminCommandCatalogIsClosedAndIsolatedFromCallers(t *testing.T) {
	service := newAdminCLIService("", "data")
	commands := service.Commands()
	seen := make(map[string]bool, len(commands))
	for _, command := range commands {
		if command.ID == "serve" || command.ID == "admin.serve" || command.ID == "help" || seen[command.ID] {
			t.Fatalf("non-finite or duplicate command: %s", command.ID)
		}
		seen[command.ID] = true
		if command.Mutates && command.Confirmation == "" {
			t.Fatalf("mutation has no review step: %s", command.ID)
		}
		for _, field := range command.Fields {
			if field.Name == "config" || field.Name == "data-dir" || field.Name == "listen" {
				t.Fatalf("source-setting field is exposed: %s/%s", command.ID, field.Name)
			}
		}
	}
	commands[0].Fields[0].Name = "data-dir"
	if service.Commands()[0].Fields[0].Name == "data-dir" {
		t.Fatal("a catalog caller changed the execution allowlist")
	}
}

func TestAdminHTTPBootstrapFromEmptyDatabaseAndPreserveOfflineLock(t *testing.T) {
	for _, name := range []string{"MAHOROBA_PROVIDER_API_KEY", "MAHOROBA_PROVIDER_API_KEY_FILE", "MAHOROBA_TTS_API_KEY", "MAHOROBA_TTS_API_KEY_FILE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	dataDir := managedCommandDataDir(t)
	service := newAdminCLIService("", dataDir)
	server, err := httpui.NewAdmin(service, httpui.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "mahoroba.db")); !os.IsNotExist(err) {
		t.Fatalf("management startup touched the database: %v", err)
	}
	page := httptest.NewRecorder()
	server.Handler().ServeHTTP(page, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8788/", nil))
	if page.Code != http.StatusOK {
		t.Fatalf("empty-database management page = %d: %s", page.Code, page.Body)
	}
	invoke := func(commandID string, overrides map[string][]string) httpui.AdminResult {
		t.Helper()
		values := make(map[string][]string)
		for _, command := range service.Commands() {
			if command.ID == commandID {
				for _, field := range command.Fields {
					if field.Default != "" {
						values[field.Name] = []string{field.Default}
					}
				}
			}
		}
		for key, value := range overrides {
			values[key] = value
		}
		body, err := json.Marshal(map[string]any{"command": commandID, "values": values, "confirmed": true})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8788/admin/execute", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "http://127.0.0.1:8788")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s HTTP %d: %s", commandID, response.Code, response.Body)
		}
		var result httpui.AdminResult
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	initResult := invoke("admin.bootstrap.init", nil)
	if initResult.ExitCode != 0 {
		t.Fatalf("init: %+v", initResult)
	}
	var state struct {
		Residents []struct {
			ID     string `json:"resident_id"`
			Status string `json:"status"`
		} `json:"residents"`
	}
	if err := json.Unmarshal([]byte(initResult.Stdout), &state); err != nil || len(state.Residents) != 1 || state.Residents[0].Status != "draft" {
		t.Fatalf("bootstrap state = %+v / %v", state, err)
	}
	resident := map[string][]string{"resident": {state.Residents[0].ID}}
	if result := invoke("admin.bootstrap.finalize", resident); result.ExitCode == 0 {
		t.Fatal("unapproved principles were finalized")
	}
	if result := invoke("admin.bootstrap.approve", resident); result.ExitCode != 0 {
		t.Fatalf("approve: %+v", result)
	}
	if result := invoke("admin.bootstrap.finalize", resident); result.ExitCode != 0 {
		t.Fatalf("finalize: %+v", result)
	}
	show := invoke("admin.resident.show", resident)
	if show.ExitCode != 0 || !strings.Contains(show.Stdout, `"status": "active"`) {
		t.Fatalf("active resident: %+v", show)
	}

	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	blocked := invoke("projection.rebuild", map[string][]string{"resident": {state.Residents[0].ID}, "all": {"true"}})
	if blocked.ExitCode == 0 || !strings.Contains(blocked.Stderr, "exclusive offline access") {
		t.Fatalf("management command bypassed the host lock: %+v", blocked)
	}
}

func TestAdminServeNeedsNeitherDatabaseNorGenerationConfiguration(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "uninitialized")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	if err := runAdminServe(ctx, []string{"--data-dir", dataDir, "--listen", "127.0.0.1:0"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Mahoroba administration: http://127.0.0.1:") {
		t.Fatalf("listener not reported: %s", stdout.String())
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("idle admin server created a data directory: %v", err)
	}
}

func TestAdminServeRejectsNonLoopbackListen(t *testing.T) {
	err := runAdminServe(context.Background(), []string{"--data-dir", filepath.Join(t.TempDir(), "unused"), "--listen", "0.0.0.0:0"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("public administration listener accepted: %v", err)
	}
}

func TestAdminCommandAdmissionHonorsCancellation(t *testing.T) {
	service := newAdminCLIService("", "unused")
	service.admission <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Execute(ctx, "db.verify", nil); err != context.Canceled {
		t.Fatalf("queued canceled command = %v", err)
	}
}
