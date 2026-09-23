package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"mahoroba.local/mahoroba/internal/config"
)

func TestDockerHostGenerationClientBypassesEnvironmentProxyAndRejectsRedirects(t *testing.T) {
	var proxyCalls, redirectedCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxy.Close()
	redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer redirected.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	for _, code := range []int{http.StatusOK, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		var directCalls atomic.Int32
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			directCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			if r.Host != "host.docker.internal:8080" || r.Header.Get("Authorization") != "Bearer fixture-key" || string(body) != "fixture-prompt" {
				t.Error("direct request changed destination or payload")
			}
			if code != http.StatusOK {
				w.Header().Set("Location", redirected.URL)
			}
			w.WriteHeader(code)
		}))
		settings := config.Generation{BaseURL: "http://host.docker.internal:8080/v1", AllowDockerHostHTTP: true}
		client := generationHTTPClient(settings)
		transport, ok := client.Transport.(*http.Transport)
		if !ok || transport.Proxy != nil || client.CheckRedirect == nil {
			t.Fatal("Docker client lost its direct no-redirect boundary")
		}
		// Only this test dialer maps the Docker endpoint to an isolated local
		// fixture. No Docker DNS, user provider or external endpoint is contacted.
		dialer := &net.Dialer{}
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "host.docker.internal:8080" {
				address = strings.TrimPrefix(provider.URL, "http://")
			}
			return dialer.DialContext(ctx, network, address)
		}
		request, err := http.NewRequest(http.MethodPost, settings.BaseURL+"/chat/completions", strings.NewReader("fixture-prompt"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer fixture-key")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		client.CloseIdleConnections()
		provider.Close()
		if response.StatusCode != code || directCalls.Load() != 1 || proxyCalls.Load() != 0 || redirectedCalls.Load() != 0 {
			t.Fatalf("code=%d: response=%d direct=%d proxy=%d redirect=%d", code, response.StatusCode, directCalls.Load(), proxyCalls.Load(), redirectedCalls.Load())
		}
	}
}

func TestGenerationClientKeepsExistingPolicyOutsideDockerHTTP(t *testing.T) {
	for _, settings := range []config.Generation{
		{BaseURL: "https://provider.example/v1"},
		{BaseURL: "http://127.0.0.1:8080/v1"},
		{BaseURL: "https://host.docker.internal:8080/v1", AllowDockerHostHTTP: true},
		{BaseURL: "http://host.docker.internal:8080/v1"},
	} {
		client := generationHTTPClient(settings)
		if client.Transport != nil || client.CheckRedirect != nil {
			t.Fatal("Docker-specific policy affected another provider")
		}
	}
}
