package httpui

import (
	"context"
	"encoding/json"
	"mime"
	"net/http"
)

// AdminRuntimeStatus describes only the dialogue runtime owned by this
// management process. A stopped state does not assert that another process is
// absent. Ready records successful startup, not a continuous health check.
type AdminRuntimeStatus struct {
	State           string `json:"state"`
	Managed         bool   `json:"managed"`
	Ready           bool   `json:"ready"`
	URL             string `json:"url"`
	Error           string `json:"error,omitempty"`
	Message         string `json:"message,omitempty"`
	RestartRequired bool   `json:"restart_required"`
}

// AdminRuntimeService keeps long-running dialogue lifecycle management separate
// from finite commands. Starting a runtime must not bind its lifetime to the
// request context; stopping may affect only a runtime owned by this service.
type AdminRuntimeService interface {
	RuntimeStatus(context.Context) (AdminRuntimeStatus, error)
	StartRuntime(context.Context) (AdminRuntimeStatus, error)
	StopRuntime(context.Context) (AdminRuntimeStatus, error)
}

func (application *adminHandler) runtimeStatus(response http.ResponseWriter, request *http.Request) {
	service, ok := application.service.(AdminRuntimeService)
	if !ok {
		writeError(response, http.StatusServiceUnavailable, "dialogue server controls are unavailable")
		return
	}
	status, err := service.RuntimeStatus(request.Context())
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (application *adminHandler) runtimeAction(response http.ResponseWriter, request *http.Request, start bool) {
	if !sameOriginMutation(request) {
		writeError(response, http.StatusForbidden, "cross-origin requests are not accepted")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1024)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input struct {
		Confirmed bool `json:"confirmed"`
	}
	if err := decoder.Decode(&input); err != nil || requireJSONEOF(decoder) != nil || !input.Confirmed {
		writeError(response, http.StatusBadRequest, "confirm the dialogue server action with a valid JSON request")
		return
	}
	service, ok := application.service.(AdminRuntimeService)
	if !ok {
		writeError(response, http.StatusServiceUnavailable, "dialogue server controls are unavailable")
		return
	}
	var status AdminRuntimeStatus
	if start {
		status, err = service.StartRuntime(request.Context())
	} else {
		status, err = service.StopRuntime(request.Context())
	}
	code := http.StatusAccepted
	if err != nil {
		status.Error = err.Error()
		code = http.StatusConflict
	}
	writeJSON(response, code, status)
}
