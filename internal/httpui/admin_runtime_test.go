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

type fakeAdminRuntimeService struct {
	fakeAdminService
	status AdminRuntimeStatus
	err    error
	reads  int
	starts int
	stops  int
}

func (service *fakeAdminRuntimeService) RuntimeStatus(context.Context) (AdminRuntimeStatus, error) {
	service.reads++
	return service.status, service.err
}

func (service *fakeAdminRuntimeService) StartRuntime(context.Context) (AdminRuntimeStatus, error) {
	service.starts++
	return service.status, service.err
}

func (service *fakeAdminRuntimeService) StopRuntime(context.Context) (AdminRuntimeStatus, error) {
	service.stops++
	return service.status, service.err
}

func TestAdminRuntimeStatusDoesNotStartOrStop(t *testing.T) {
	service := &fakeAdminRuntimeService{status: AdminRuntimeStatus{
		State: "failed", URL: "http://127.0.0.1:8787/", Error: "resident memory policy is not ready",
		Message: "Review the current resident configuration.",
	}}
	server, err := NewAdmin(service, Options{})
	if err != nil {
		t.Fatal(err)
	}
	response := adminTestRequest(server.Handler(), http.MethodGet, "/admin/runtime", "")
	var status AdminRuntimeStatus
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &status) != nil || status != service.status {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	if service.reads != 1 || service.starts != 0 || service.stops != 0 || service.calls != 0 {
		t.Fatalf("runtime status triggered mutation: %+v", service)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status must not be cached: %v", response.Header())
	}
}

func TestAdminRuntimeActionsPreserveAsyncAndFailureStates(t *testing.T) {
	for _, test := range []struct {
		name   string
		action string
		status AdminRuntimeStatus
		err    error
		code   int
	}{
		{name: "start accepted", action: "start", status: AdminRuntimeStatus{State: "starting", Managed: true}, code: http.StatusAccepted},
		{name: "stop accepted", action: "stop", status: AdminRuntimeStatus{State: "stopping", Managed: true}, code: http.StatusAccepted},
		{name: "refused start keeps state", action: "start", status: AdminRuntimeStatus{State: "running", Managed: true, Ready: true}, err: errors.New("already running"), code: http.StatusConflict},
		{name: "shutdown boundary keeps restart required", action: "stop", status: AdminRuntimeStatus{State: "failed", Managed: true, RestartRequired: true}, err: errors.New("shutdown did not complete"), code: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAdminRuntimeService{status: test.status, err: test.err}
			server, err := NewAdmin(service, Options{})
			if err != nil {
				t.Fatal(err)
			}
			response := adminTestRequest(server.Handler(), http.MethodPost, "/admin/runtime/"+test.action, `{"confirmed":true}`)
			var status AdminRuntimeStatus
			if response.Code != test.code || json.Unmarshal(response.Body.Bytes(), &status) != nil || status.State != test.status.State ||
				status.Managed != test.status.Managed || status.Ready != test.status.Ready || status.RestartRequired != test.status.RestartRequired {
				t.Fatalf("code=%d body=%s", response.Code, response.Body)
			}
			if test.err != nil && status.Error != test.err.Error() {
				t.Fatalf("error not preserved: %+v", status)
			}
			if service.starts+service.stops != 1 || (test.action == "start" && service.starts != 1) || service.calls != 0 {
				t.Fatalf("wrong dispatch: %+v", service)
			}
		})
	}
}

func TestAdminRuntimeActionsRejectInvalidInputBeforeExecution(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		for _, test := range []struct {
			name string
			body string
		}{
			{"empty body", ""},
			{"missing confirmation", `{}`},
			{"false confirmation", `{"confirmed":false}`},
			{"invalid confirmation type", `{"confirmed":"true"}`},
			{"unknown field", `{"confirmed":true,"data_dir":"other"}`},
			{"additional JSON", `{"confirmed":true}{}`},
			{"null", `null`},
			{"oversize", `{"confirmed":true}` + strings.Repeat(" ", 1024)},
		} {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				service := &fakeAdminRuntimeService{}
				server, err := NewAdmin(service, Options{})
				if err != nil {
					t.Fatal(err)
				}
				response := adminTestRequest(server.Handler(), http.MethodPost, "/admin/runtime/"+action, test.body)
				if response.Code != http.StatusBadRequest || service.starts+service.stops != 0 {
					t.Fatalf("code=%d body=%s starts=%d stops=%d", response.Code, response.Body, service.starts, service.stops)
				}
			})
		}
	}
}

func TestAdminRuntimeActionsPreserveOriginHostAndMethodBoundary(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		for _, test := range []struct {
			name        string
			method      string
			host        string
			origin      string
			contentType string
			code        int
		}{
			{name: "missing origin", method: http.MethodPost, host: "127.0.0.1:8788", contentType: "application/json", code: http.StatusForbidden},
			{name: "cross origin", method: http.MethodPost, host: "127.0.0.1:8788", origin: "http://other.example", contentType: "application/json", code: http.StatusForbidden},
			{name: "remote host", method: http.MethodPost, host: "other.example", origin: "http://other.example", contentType: "application/json", code: http.StatusMisdirectedRequest},
			{name: "form content", method: http.MethodPost, host: "127.0.0.1:8788", origin: "http://127.0.0.1:8788", contentType: "application/x-www-form-urlencoded", code: http.StatusBadRequest},
			{name: "GET cannot mutate", method: http.MethodGet, host: "127.0.0.1:8788", origin: "http://127.0.0.1:8788", contentType: "application/json", code: http.StatusMethodNotAllowed},
		} {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				service := &fakeAdminRuntimeService{}
				server, err := NewAdmin(service, Options{})
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(test.method, "/admin/runtime/"+action, strings.NewReader(`{"confirmed":true}`))
				request.Host = test.host
				request.Header.Set("Origin", test.origin)
				request.Header.Set("Content-Type", test.contentType)
				response := httptest.NewRecorder()
				server.Handler().ServeHTTP(response, request)
				if response.Code != test.code || service.starts+service.stops != 0 {
					t.Fatalf("code=%d body=%s starts=%d stops=%d", response.Code, response.Body, service.starts, service.stops)
				}
			})
		}
	}
}

func TestAdminWithoutRuntimeCapabilityRejectsControls(t *testing.T) {
	service := &fakeAdminService{}
	server, err := NewAdmin(service, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/admin/runtime", "/admin/runtime/start", "/admin/runtime/stop"} {
		method, body := http.MethodPost, `{"confirmed":true}`
		if path == "/admin/runtime" {
			method, body = http.MethodGet, ""
		}
		response := adminTestRequest(server.Handler(), method, path, body)
		if response.Code != http.StatusServiceUnavailable || service.calls != 0 {
			t.Fatalf("path=%s code=%d body=%s", path, response.Code, response.Body)
		}
	}
}
