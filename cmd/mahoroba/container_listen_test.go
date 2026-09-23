package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/httpui"
)

func TestAdminContainerFlagPropagatesWithoutRemoteHostPermission(t *testing.T) {
	for _, container := range []bool{false, true} {
		for _, remote := range []bool{false, true} {
			flags := flag.NewFlagSet("serve", flag.ContinueOnError)
			common := addCommon(flags, true)
			args := adminDialogueServeArguments("example.toml", "example-data", remote, container)
			if err := flags.Parse(args); err != nil {
				t.Fatal(err)
			}
			if *common.containerListen != container || *common.allowRemote != remote || *common.configPath != "example.toml" || *common.dataDir != "example-data" {
				t.Fatalf("managed dialogue arguments changed permissions: %#v", args)
			}
		}
	}
	for address, expected := range map[string]string{
		"0.0.0.0:19888":   "http://127.0.0.1:19888",
		"[::]:19887":      "http://[::1]:19887",
		"127.0.0.1:19886": "http://127.0.0.1:19886",
	} {
		if got := dialogueConnectURL(address); got != expected {
			t.Fatalf("%s: %s != %s", address, got, expected)
		}
		if got := localUIConnectURL(address, false); got != expected {
			t.Fatalf("default URL changed for %s: %s", address, got)
		}
	}
	for _, address := range []string{"0.0.0.0:19887", "[::]:19887", "172.18.0.2:19887"} {
		if got := localUIConnectURL(address, true); got != "http://127.0.0.1:19887" {
			t.Fatalf("container URL points at an unpublished address: %s", got)
		}
	}
}

func TestAdminHealthcheckRequiresItsOwnExactLivenessResponse(t *testing.T) {
	server, err := httpui.NewAdmin(newAdminCLIService("", "unused"), httpui.Options{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(server.Handler())
	defer endpoint.Close()
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"admin", "healthcheck", "--url", endpoint.URL + "/healthz"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "administration healthy") {
		t.Fatalf("admin probe = %d / %s / %s", code, &stdout, &stderr)
	}
	// A liveness success must never become a dialogue readiness observation.
	if code := runHealthcheck(context.Background(), []string{"--url", endpoint.URL + "/healthz"}, io.Discard, io.Discard); code == 0 {
		t.Fatal("dialogue probe accepted management liveness")
	}
	for _, test := range []struct {
		status            int
		contentType, body string
	}{
		{http.StatusOK, "application/json", `{\"ready\":true}`},
		{http.StatusOK, "application/json", httpui.AdminHealthBody + " "},
		{http.StatusOK, "text/plain", httpui.AdminHealthBody},
		{http.StatusServiceUnavailable, "application/json", httpui.AdminHealthBody},
		{http.StatusFound, "application/json", httpui.AdminHealthBody},
	} {
		fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", test.contentType)
			w.Header().Set("Location", endpoint.URL+"/healthz")
			w.WriteHeader(test.status)
			_, _ = io.WriteString(w, test.body)
		}))
		err := runAdminHealthcheck(context.Background(), []string{"--url", fixture.URL + "/healthz"}, io.Discard, io.Discard)
		fixture.Close()
		if err == nil {
			t.Fatalf("accepted invalid admin health response: %+v", test)
		}
	}
	for _, arguments := range [][]string{
		{"--url", "http://192.0.2.1:8788/healthz"},
		{"--url", "http://127.0.0.1:8788/"},
		{"--url", "http://127.0.0.1:8788/healthz", "--url", endpoint.URL + "/healthz"},
		{"--url", ""}, {"--config", "unused"}, {"extra"},
	} {
		if err := runAdminHealthcheck(context.Background(), arguments, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted invalid arguments: %q", arguments)
		}
	}
}
