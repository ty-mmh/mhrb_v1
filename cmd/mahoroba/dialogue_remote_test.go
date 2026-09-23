package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/httpui"
)

func TestDialogueConnectURLPreservesSpecificInterfaces(t *testing.T) {
	for address, want := range map[string]string{
		"0.0.0.0:8787":     "http://127.0.0.1:8787",
		"[::]:8787":        "http://[::1]:8787",
		"127.0.0.1:8787":   "http://127.0.0.1:8787",
		"100.64.0.42:8787": "http://100.64.0.42:8787",
	} {
		if got := dialogueConnectURL(address); got != want {
			t.Fatalf("%s => %s, want %s", address, got, want)
		}
	}
	for _, address := range []string{"0.0.0.0:8787", "[::]:8787"} {
		if _, _, err := normalizeHealthcheckURL(dialogueConnectURL(address) + "/healthz"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := normalizeHealthcheckURL(dialogueConnectURL("100.64.0.42:8787") + "/healthz"); err == nil {
		t.Fatal("specific remote interface bypassed the healthcheck loopback boundary")
	}
}

func TestDialogueRemoteFlagDoesNotExposeAdministration(t *testing.T) {
	err := runAdminServe(context.Background(), []string{
		"--data-dir", filepath.Join(t.TempDir(), "unused"), "--listen", "0.0.0.0:0", "--dialogue-allow-remote",
	}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("remote administration accepted: %v", err)
	}
}

type dialogueTestAnnouncements chan string

func (out dialogueTestAnnouncements) Write(data []byte) (int, error) {
	select {
	case out <- string(data):
	default:
	}
	return len(data), nil
}

func TestAdminDialogueRemoteFlagIsScopedAndForwarded(t *testing.T) {
	for _, name := range []string{"MAHOROBA_LISTEN", "MAHOROBA_PROVIDER_API_KEY", "MAHOROBA_PROVIDER_API_KEY_FILE", "MAHOROBA_TTS_API_KEY", "MAHOROBA_TTS_API_KEY_FILE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("MAHOROBA_PROVIDER_API_KEY", "test-key")
	dataDir := managedCommandDataDir(t)
	configPath := filepath.Join(t.TempDir(), "dialogue.toml")
	writeConfig := func(listen string) {
		t.Helper()
		text := fmt.Sprintf("data_dir = %q\n[server]\nlisten = %q\nshutdown_timeout = \"2s\"\n[generation]\nbase_url = \"http://127.0.0.1:1/v1\"\nmodel = \"test-model\"\n", dataDir, listen)
		if err := os.WriteFile(configPath, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The shared configuration can describe a remote dialogue listener without
	// granting a finite administration command permission to open that listener.
	writeConfig("100.64.0.42:8787")
	service := newAdminCLIService(configPath, dataDir)
	result, err := service.Execute(context.Background(), "admin.bootstrap.init", nil)
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("remote-config bootstrap: %+v / %v", result, err)
	}
	resident := adminTestResidentID(t, result.Stdout)
	for _, command := range []string{"admin.bootstrap.approve", "admin.bootstrap.finalize"} {
		result, err := service.Execute(context.Background(), command, map[string][]string{"resident": {resident}})
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("%s: %+v / %v", command, result, err)
		}
	}
	if err := runServe(context.Background(), []string{"--config", configPath}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "not a loopback") {
		t.Fatalf("remote serve without flag did not fail before startup: %v", err)
	}
	// Exercise the exact Tailscale Serve arrangement without exposing a test
	// listener: a loopback connection carrying a custom Host and HTTPS Origin.
	writeConfig("127.0.0.1:0")
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow_remote_%v", allow), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			announcements := make(dialogueTestAnnouncements, 1)
			args := []string{"--config", configPath, "--listen", "127.0.0.1:0"}
			if allow {
				args = append(args, "--dialogue-allow-remote")
			}
			go func() { done <- runAdminServe(ctx, args, announcements, io.Discard) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(12 * time.Second):
					t.Error("administration did not join its dialogue runtime")
				}
			}()
			var adminURL string
			select {
			case line := <-announcements:
				adminURL = strings.TrimSpace(strings.TrimPrefix(line, "Mahoroba administration: "))
			case <-time.After(10 * time.Second):
				t.Fatal("administration listener was not announced")
			}
			client := &http.Client{Timeout: 3 * time.Second}
			request, _ := http.NewRequest(http.MethodPost, adminURL+"admin/runtime/start", bytes.NewBufferString("{\"confirmed\":true}"))
			request.Header.Set("Origin", strings.TrimSuffix(adminURL, "/"))
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusAccepted {
				t.Fatalf("start: %d %s", response.StatusCode, body)
			}
			var status httpui.AdminRuntimeStatus
			deadline := time.Now().Add(15 * time.Second)
			for {
				response, err := client.Get(adminURL + "admin/runtime")
				if err != nil {
					t.Fatal(err)
				}
				err = json.NewDecoder(response.Body).Decode(&status)
				_ = response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if status.State == "running" {
					break
				}
				if status.State == "failed" || time.Now().After(deadline) {
					t.Fatalf("managed startup: %+v", status)
				}
				time.Sleep(10 * time.Millisecond)
			}
			for _, check := range []struct {
				url  string
				want int
			}{
				{adminURL, http.StatusMisdirectedRequest},
				{status.URL, map[bool]int{false: http.StatusMisdirectedRequest, true: http.StatusOK}[allow]},
			} {
				request, _ := http.NewRequest(http.MethodGet, check.url, nil)
				request.Host = "resident.example.ts.net"
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != check.want {
					t.Fatalf("%s custom Host: %d, want %d", check.url, response.StatusCode, check.want)
				}
			}
			request, _ = http.NewRequest(http.MethodPost, status.URL+"messages", strings.NewReader("{}"))
			request.Host = "resident.example.ts.net"
			request.Header.Set("Origin", "https://resident.example.ts.net")
			request.Header.Set("Content-Type", "application/json")
			response, err = client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			want := http.StatusMisdirectedRequest
			if allow {
				want = http.StatusBadRequest
			} // Origin passed; empty input cannot create a message.
			if response.StatusCode != want {
				t.Fatalf("proxied HTTPS Origin: %d, want %d", response.StatusCode, want)
			}
		})
	}
}
