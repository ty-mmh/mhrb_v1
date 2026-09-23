package httpui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

// AdminHealthBody identifies management liveness without claiming dialogue readiness.
const AdminHealthBody = "{\"service\":\"administration\",\"status\":\"ok\"}\n"

// AdminCommand describes one closed, finite CLI operation. It is supplied by
// the application; browsers cannot supply command lines or runtime settings.
type AdminCommand struct {
	ID           string       `json:"id"`
	Label        string       `json:"label"`
	Group        string       `json:"group"`
	Description  string       `json:"description"`
	Fields       []AdminField `json:"fields"`
	Mutates      bool         `json:"mutates"`
	Offline      bool         `json:"offline"`
	Confirmation string       `json:"confirmation,omitempty"`
}

type AdminField struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Type        string   `json:"type"`
	Default     string   `json:"default,omitempty"`
	Required    bool     `json:"required"`
	Repeatable  bool     `json:"repeatable"`
	Choices     []string `json:"choices,omitempty"`
	Description string   `json:"description,omitempty"`
}

type AdminResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type AdminService interface {
	Commands() []AdminCommand
	Execute(context.Context, string, map[string][]string) (AdminResult, error)
}

type adminHandler struct {
	service  AdminService
	commands []AdminCommand
	page     *template.Template
	static   http.Handler
	options  Options
}

// NewAdmin creates a separate management server restricted to loopback Hosts.
// ContainerListen changes only its socket binding. Finite commands and
// optional dialogue lifecycle controls retain the application's ownership,
// host-lock and validation boundaries.
func NewAdmin(service AdminService, options Options) (*Server, error) {
	if service == nil {
		return nil, errors.New("httpui: nil admin service")
	}
	if options.DialogueAllowRemote {
		return nil, errors.New("httpui: remote dialogue access cannot be enabled on the management server")
	}
	if options.Address == "" {
		options.Address = "127.0.0.1:8788"
	}
	if options.DialogueURL == "" {
		options.DialogueURL = "http://127.0.0.1:8787/"
	}
	validated, err := options.validate()
	if err != nil {
		return nil, err
	}
	page, err := template.ParseFS(assets, "templates/admin.html")
	if err != nil {
		return nil, fmt.Errorf("httpui: parse admin template: %w", err)
	}
	staticRoot, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	application := &adminHandler{
		service: service, commands: service.Commands(), page: page,
		static: http.StripPrefix("/static/", http.FileServer(http.FS(staticRoot))), options: validated,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", application.index)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		// Management is usable before bootstrap and while dialogue is stopped.
		// This is liveness only, not the dialogue captured-head readiness contract.
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(response, AdminHealthBody)
	})
	mux.HandleFunc("GET /admin/commands", application.catalog)
	mux.HandleFunc("POST /admin/execute", application.execute)
	mux.HandleFunc("GET /admin/runtime", application.runtimeStatus)
	mux.HandleFunc("POST /admin/runtime/start", func(response http.ResponseWriter, request *http.Request) {
		application.runtimeAction(response, request, true)
	})
	mux.HandleFunc("POST /admin/runtime/stop", func(response http.ResponseWriter, request *http.Request) {
		application.runtimeAction(response, request, false)
	})
	mux.HandleFunc("GET /static/", func(response http.ResponseWriter, request *http.Request) {
		name := strings.TrimPrefix(request.URL.Path, "/static/")
		if name == "" || strings.HasSuffix(name, "/") {
			http.NotFound(response, request)
			return
		}
		application.static.ServeHTTP(response, request)
	})
	return newServer(securityHeaders(mux), NewHub(1), validated), nil
}

func (application *adminHandler) index(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	if err := application.page.ExecuteTemplate(response, "admin.html", application.options); err != nil {
		application.options.Logger.ErrorContext(request.Context(), "render management UI", "error", err)
	}
}

func (application *adminHandler) catalog(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, application.commands)
}

func (application *adminHandler) execute(response http.ResponseWriter, request *http.Request) {
	if !sameOriginMutation(request) {
		writeError(response, http.StatusForbidden, "cross-origin requests are not accepted")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(response, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1024*1024)
	contents, err := io.ReadAll(request.Body)
	if err != nil || !utf8.Valid(contents) {
		writeError(response, http.StatusBadRequest, "request body is too large or invalid UTF-8")
		return
	}
	var input struct {
		Command   string              `json:"command"`
		Values    map[string][]string `json:"values"`
		Confirmed bool                `json:"confirmed"`
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || requireJSONEOF(decoder) != nil {
		writeError(response, http.StatusBadRequest, "invalid management request")
		return
	}
	var command *AdminCommand
	for index := range application.commands {
		if application.commands[index].ID == input.Command {
			command = &application.commands[index]
			break
		}
	}
	if command == nil {
		writeError(response, http.StatusBadRequest, "unknown operation")
		return
	}
	if command.Confirmation != "" && !input.Confirmed {
		writeError(response, http.StatusBadRequest, "review and confirm this operation before executing it")
		return
	}
	if err := validateAdminValues(*command, input.Values); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	result, err := application.service.Execute(request.Context(), input.Command, input.Values)
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, err.Error())
		return
	}
	// A command's exit code and complete diagnostics are the operation result.
	// In particular, nonzero/partial CLI outcomes must not be called a success.
	writeJSON(response, http.StatusOK, result)
}

func validateAdminValues(command AdminCommand, values map[string][]string) error {
	fields := make(map[string]AdminField, len(command.Fields))
	for _, field := range command.Fields {
		fields[field.Name] = field
		if field.Required && (len(values[field.Name]) == 0 || strings.TrimSpace(values[field.Name][0]) == "") {
			return fmt.Errorf("%s is required", field.Label)
		}
	}
	for name, items := range values {
		field, ok := fields[name]
		if !ok {
			return fmt.Errorf("unknown field %q", name)
		}
		if len(items) == 0 || !field.Repeatable && len(items) != 1 {
			return fmt.Errorf("invalid number of values for %s", field.Label)
		}
		for _, value := range items {
			if strings.ContainsRune(value, 0) {
				return fmt.Errorf("%s must not contain NUL", field.Label)
			}
			if field.Type == "checkbox" && value != "true" && value != "false" {
				return fmt.Errorf("%s must be true or false", field.Label)
			}
			if len(field.Choices) != 0 {
				found := false
				for _, choice := range field.Choices {
					found = found || choice == value
				}
				if !found {
					return fmt.Errorf("invalid choice for %s", field.Label)
				}
			}
		}
	}
	return nil
}
