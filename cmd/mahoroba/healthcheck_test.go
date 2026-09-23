package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/healthwire"
	"mahoroba.local/mahoroba/internal/readiness"
)

func TestM7HealthcheckReadyAndNotReadyUseStrictCLIEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wire       healthwire.Response
		wantExit   int
		wantStream string
	}{
		{
			name: "ready", status: http.StatusOK, wantExit: 0, wantStream: "stdout",
			wire: healthwire.Response{CheckedAtUnixMicros: "1787220000000000", FormatVersion: healthwire.FormatVersion, Ready: true, ReasonCode: readiness.ReasonReady},
		},
		{
			name: "not ready", status: http.StatusServiceUnavailable, wantExit: 1, wantStream: "stderr",
			wire: healthwire.Response{CheckedAtUnixMicros: "1787220000000000", FormatVersion: healthwire.FormatVersion, Ready: false, ReasonCode: readiness.ReasonIntegrityBlocked},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/healthz" {
					t.Fatalf("path = %q", request.URL.Path)
				}
				body, err := healthwire.MarshalExact(test.wire)
				if err != nil {
					t.Fatal(err)
				}
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(test.status)
				_, _ = response.Write(body)
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			exit := runHealthcheck(context.Background(), []string{"--url", server.URL + "/healthz"}, &stdout, &stderr)
			if exit != test.wantExit {
				t.Fatalf("exit = %d, stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
			}
			selected, other := stdout.String(), stderr.String()
			if test.wantStream == "stderr" {
				selected, other = stderr.String(), stdout.String()
			}
			if other != "" || !strings.Contains(selected, `"checked_at_unix_micros":"1787220000000000"`) ||
				!strings.Contains(selected, `"command":"healthcheck"`) {
				t.Fatalf("unexpected output stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestM7HealthcheckRejectsResponseMatrixAndNonLoopback(t *testing.T) {
	ready, err := healthwire.MarshalExact(healthwire.Response{
		CheckedAtUnixMicros: "1", FormatVersion: healthwire.FormatVersion,
		Ready: true, ReasonCode: readiness.ReasonReady,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		status      int
		contentType string
		body        []byte
	}{
		{name: "status mismatch", status: http.StatusServiceUnavailable, contentType: "application/json", body: ready},
		{name: "unknown status", status: http.StatusCreated, contentType: "application/json", body: ready},
		{name: "charset forbidden", status: http.StatusOK, contentType: "application/json; charset=utf-8", body: ready},
		{name: "malformed", status: http.StatusOK, contentType: "application/json", body: []byte("{}\n")},
		{name: "oversize", status: http.StatusOK, contentType: "application/json", body: bytes.Repeat([]byte{'x'}, healthcheckMaximumBody+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.contentType)
				response.WriteHeader(test.status)
				_, _ = response.Write(test.body)
			}))
			defer server.Close()
			endpoint, address, err := normalizeHealthcheckURL(server.URL + "/healthz")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := requestHealthcheck(context.Background(), endpoint, newHealthcheckClient(address)); !errors.Is(err, errHealthInvalid) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	for _, raw := range []string{
		"https://127.0.0.1:8787/healthz", "http://192.0.2.1:8787/healthz",
		"http://127.0.0.1:8787/", "http://127.0.0.1:8787/healthz?x=1",
		"http://user@127.0.0.1:8787/healthz", "http://127.0.0.1/healthz",
	} {
		if _, _, err := normalizeHealthcheckURL(raw); err == nil {
			t.Fatalf("invalid URL accepted: %q", raw)
		}
	}
}

func TestM7HealthcheckRedirectAndSilentServerFailClosed(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/healthz", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	endpoint, address, err := normalizeHealthcheckURL(redirect.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requestHealthcheck(context.Background(), endpoint, newHealthcheckClient(address)); !errors.Is(err, errHealthInvalid) {
		t.Fatalf("redirect error = %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()
	silentURL := "http://" + listener.Addr().String() + "/healthz"
	silentEndpoint, silentAddress, err := normalizeHealthcheckURL(silentURL)
	if err != nil {
		t.Fatal(err)
	}
	client := newHealthcheckClient(silentAddress)
	client.Timeout = 75 * time.Millisecond
	if _, err := requestHealthcheck(context.Background(), silentEndpoint, client); !errors.Is(err, errHealthUnreachable) {
		t.Fatalf("silent server error = %v", err)
	}
	select {
	case connection := <-accepted:
		_ = connection.Close()
	case <-time.After(time.Second):
	}
}

func TestM7HealthcheckRejectsDuplicateAndUnknownArguments(t *testing.T) {
	for _, arguments := range [][]string{
		{"--url", "http://127.0.0.1:1/healthz", "--url", "http://127.0.0.1:2/healthz"},
		{"--config", ""}, {"--url", ""}, {"--data-dir", `C:\data`}, {"extra"},
	} {
		var stdout, stderr bytes.Buffer
		if exit := runHealthcheck(context.Background(), arguments, &stdout, &stderr); exit != 2 || stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), "usage: mahoroba healthcheck") {
			t.Fatalf("args=%q exit/output = %d %q %q", arguments, exit, stdout.String(), stderr.String())
		}
	}
}
