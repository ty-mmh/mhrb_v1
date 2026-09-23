package httpui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAdminService struct {
	calls   int
	command string
	values  map[string][]string
	result  AdminResult
	err     error
}

func (service *fakeAdminService) Commands() []AdminCommand {
	return []AdminCommand{
		{ID: "inspect", Label: "Inspect", Group: "Residents"},
		{
			ID: "change", Label: "Change", Group: "Residents", Mutates: true, Offline: true,
			Confirmation: "I reviewed this change.",
			Fields: []AdminField{
				{Name: "resident", Label: "Resident ID", Type: "text", Required: true},
				{Name: "item", Label: "Item", Type: "textarea", Repeatable: true},
				{Name: "select", Label: "Select resident", Type: "checkbox", Default: "true"},
				{Name: "mode", Label: "Mode", Type: "select", Choices: []string{"one", "two"}},
			},
		},
	}
}

func (service *fakeAdminService) Execute(_ context.Context, command string, values map[string][]string) (AdminResult, error) {
	service.calls++
	service.command = command
	service.values = values
	return service.result, service.err
}

func adminTestRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Host = "127.0.0.1:8788"
	request.Header.Set("Origin", "http://127.0.0.1:8788")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestAdminPageAndCatalogNeedNoActiveResident(t *testing.T) {
	service := &fakeAdminService{}
	server, err := NewAdmin(service, Options{DialogueURL: "http://127.0.0.1:9876/"})
	if err != nil {
		t.Fatal(err)
	}
	if server.Address() != "127.0.0.1:8788" {
		t.Fatalf("management address = %q", server.Address())
	}
	response := adminTestRequest(server.Handler(), http.MethodGet, "/", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Bootstrap approve") ||
		!strings.Contains(response.Body.String(), "http://127.0.0.1:9876/") {
		t.Fatalf("page status=%d body=%s", response.Code, response.Body)
	}
	if response.Header().Get("Cache-Control") != "no-store" || !strings.Contains(response.Header().Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatalf("security headers = %v", response.Header())
	}
	response = adminTestRequest(server.Handler(), http.MethodGet, "/admin/commands", "")
	var commands []AdminCommand
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &commands) != nil ||
		len(commands) != 2 || commands[1].Confirmation == "" || service.calls != 0 {
		t.Fatalf("catalog status=%d body=%s calls=%d", response.Code, response.Body, service.calls)
	}
	for _, path := range []string{"/admin/unknown", "/messages", "/static/"} {
		if response := adminTestRequest(server.Handler(), http.MethodGet, path, ""); response.Code != http.StatusNotFound {
			t.Fatalf("unexpected route %s: %d", path, response.Code)
		}
	}
}

func TestAdminReturnsCommandOutputAndNonzeroExitCode(t *testing.T) {
	service := &fakeAdminService{result: AdminResult{ExitCode: 3, Stdout: `{"canonical_applied":true}`, Stderr: "projection rebuild required"}}
	server, err := NewAdmin(service, Options{})
	if err != nil {
		t.Fatal(err)
	}
	response := adminTestRequest(server.Handler(), http.MethodPost, "/admin/execute",
		`{"command":"change","values":{"resident":["resident-id"],"item":["first","second"],"select":["false"]},"confirmed":true}`)
	var result AdminResult
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result != service.result {
		t.Fatalf("command result status=%d body=%s", response.Code, response.Body)
	}
	if service.calls != 1 || service.command != "change" || len(service.values["item"]) != 2 || service.values["select"][0] != "false" {
		t.Fatalf("executed command=%q values=%v calls=%d", service.command, service.values, service.calls)
	}
}

func TestAdminRejectsInvalidRequestsBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"invalid JSON", "{"},
		{"extra JSON", `{"command":"inspect"} {}`},
		{"unknown outer field", `{"command":"inspect","args":[]}`},
		{"unknown command", `{"command":"serve"}`},
		{"unknown field", `{"command":"inspect","values":{"data-dir":["other"]}}`},
		{"confirmation missing", `{"command":"change","values":{"resident":["id"]}}`},
		{"required missing", `{"command":"change","confirmed":true}`},
		{"required empty", `{"command":"change","values":{"resident":[" "]},"confirmed":true}`},
		{"empty values", `{"command":"change","values":{"resident":[],"item":[]},"confirmed":true}`},
		{"repeated scalar", `{"command":"change","values":{"resident":["first","second"]},"confirmed":true}`},
		{"invalid choice", `{"command":"change","values":{"resident":["id"],"mode":["three"]},"confirmed":true}`},
		{"invalid checkbox", `{"command":"change","values":{"resident":["id"],"select":["yes"]},"confirmed":true}`},
		{"NUL", `{"command":"change","values":{"resident":["a\u0000b"]},"confirmed":true}`},
		{"wrong value type", `{"command":"change","values":{"resident":"id"},"confirmed":true}`},
		{"oversize", strings.Repeat("x", 1024*1024+1)},
		{"invalid UTF-8", string([]byte{0xff})},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAdminService{}
			server, err := NewAdmin(service, Options{})
			if err != nil {
				t.Fatal(err)
			}
			response := adminTestRequest(server.Handler(), http.MethodPost, "/admin/execute", test.body)
			if response.Code != http.StatusBadRequest || service.calls != 0 {
				t.Fatalf("status=%d body=%s calls=%d", response.Code, response.Body, service.calls)
			}
		})
	}
}

func TestAdminPreservesLoopbackAndSameOriginBoundary(t *testing.T) {
	for _, test := range []struct {
		name        string
		host        string
		origin      string
		contentType string
		fetchSite   string
		fetchMode   string
		want        int
	}{
		{name: "same origin", origin: "http://127.0.0.1:8788", want: http.StatusOK},
		{name: "missing origin", want: http.StatusForbidden},
		{name: "cross origin", origin: "https://other.example", want: http.StatusForbidden},
		{name: "remote host", host: "other.example", origin: "http://other.example", want: http.StatusMisdirectedRequest},
		{name: "same origin metadata", fetchSite: "same-origin", fetchMode: "cors", want: http.StatusOK},
		{name: "cross site metadata", fetchSite: "cross-site", fetchMode: "cors", want: http.StatusForbidden},
		{name: "form content type", origin: "http://127.0.0.1:8788", contentType: "application/x-www-form-urlencoded", want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAdminService{}
			server, err := NewAdmin(service, Options{})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/admin/execute", strings.NewReader(`{"command":"inspect"}`))
			request.Host = "127.0.0.1:8788"
			if test.host != "" {
				request.Host = test.host
			}
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Content-Type", "application/json")
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			request.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != test.want || service.calls != map[bool]int{true: 1}[test.want == http.StatusOK] {
				t.Fatalf("status=%d want=%d calls=%d body=%s", response.Code, test.want, service.calls, response.Body)
			}
		})
	}
	if _, err := NewAdmin(&fakeAdminService{}, Options{Address: "0.0.0.0:8788"}); err == nil {
		t.Fatal("management server accepted a public listener")
	}
}

func TestAdminServiceFailureIsNotSuccess(t *testing.T) {
	service := &fakeAdminService{err: errors.New("another management operation is running")}
	server, err := NewAdmin(service, Options{})
	if err != nil {
		t.Fatal(err)
	}
	response := adminTestRequest(server.Handler(), http.MethodPost, "/admin/execute", `{"command":"inspect"}`)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "another management operation") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
}
