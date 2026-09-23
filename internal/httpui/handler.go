package httpui

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/healthwire"
	"mahoroba.local/mahoroba/internal/readiness"
)

// Service is the entire authority visible to the local UI. Ingress performs a
// canonical write; History and ActiveResident read canonical state.
type Service interface {
	IngressWithMetadata(context.Context, domain.IngressRequest) (domain.Event, error)
	History(context.Context, canonical.ID, int) ([]domain.Event, error)
	ActiveResident(context.Context) (domain.ResidentSnapshot, error)
}

//go:embed templates/*.html static/*
var assets embed.FS

type handler struct {
	service Service
	hub     *Hub
	options Options
	index   *template.Template
	static  http.Handler
}

type pageData struct {
	Title          string
	Ready          bool
	ResidentName   string
	ResidentID     string
	ResidentStatus string
	StatusMessage  string
	AudioAvailable bool
	Messages       []messageView
	ManagementURL  string
}

type messageView struct {
	EventID string
	Role    string
	Label   string
	Text    string
	Time    string
}

func newHandler(service Service, hub *Hub, options Options) (http.Handler, error) {
	index, err := template.ParseFS(assets, "templates/index.html")
	if err != nil {
		return nil, fmt.Errorf("httpui: parse index template: %w", err)
	}
	staticRoot, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, fmt.Errorf("httpui: open static assets: %w", err)
	}
	application := &handler{
		service: service,
		hub:     hub,
		options: options,
		index:   index,
		static:  http.StripPrefix("/static/", http.FileServer(http.FS(staticRoot))),
	}
	mux := http.NewServeMux()
	// The root page is exact. A catch-all GET / route would silently publish
	// future-looking surfaces (for example /branch) as the ordinary UI.
	mux.HandleFunc("GET /{$}", application.indexPage)
	mux.HandleFunc("POST /messages", application.postMessage)
	mux.HandleFunc("GET /events", application.events)
	mux.HandleFunc("GET /healthz", application.health)
	mux.HandleFunc("GET /static/", application.staticAsset)
	return dialogueSecurityHeaders(mux, options.DialogueAllowRemote), nil
}

func (application *handler) indexPage(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(response, request)
		return
	}
	managementURL := application.options.ManagementURL
	if managementURL == "" {
		managementURL = "http://127.0.0.1:8788/"
	}
	data := pageData{Title: "Mahoroba", AudioAvailable: application.options.AudioEnabled, ManagementURL: managementURL}
	resident, err := application.service.ActiveResident(request.Context())
	status := http.StatusOK
	if err != nil {
		status = http.StatusServiceUnavailable
		data.StatusMessage = "No active resident is available. Complete bootstrap or select an active resident."
		application.options.Logger.WarnContext(request.Context(), "load active resident for UI", "error", err)
	} else {
		data.Ready = resident.Status == "active"
		data.ResidentName = validText(resident.Name)
		data.ResidentID = resident.ResidentID.String()
		data.ResidentStatus = validText(resident.Status)
		if !data.Ready {
			status = http.StatusServiceUnavailable
			data.StatusMessage = "The selected resident is not active."
		} else {
			events, historyErr := application.service.History(request.Context(), resident.ResidentID, application.options.HistoryLimit)
			if historyErr != nil {
				status = http.StatusServiceUnavailable
				data.Ready = false
				data.StatusMessage = "Canonical dialogue history is temporarily unavailable."
				application.options.Logger.ErrorContext(request.Context(), "load dialogue history for UI", "error", historyErr)
			} else {
				data.Messages = make([]messageView, 0, len(events))
				for _, event := range events {
					if presented, ok := presentEvent(event, resident.Name); ok {
						data.Messages = append(data.Messages, presented)
					}
				}
			}
		}
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	if err := application.index.ExecuteTemplate(response, "index.html", data); err != nil {
		application.options.Logger.ErrorContext(request.Context(), "render UI", "error", err)
	}
}

func (application *handler) staticAsset(response http.ResponseWriter, request *http.Request) {
	assetName := strings.TrimPrefix(request.URL.Path, "/static/")
	if assetName == "" || strings.HasSuffix(assetName, "/") {
		http.NotFound(response, request)
		return
	}
	application.static.ServeHTTP(response, request)
}

func presentEvent(event domain.Event, residentName string) (messageView, bool) {
	view := messageView{
		EventID: event.ID.String(),
		Role:    "system",
		Label:   "System",
		Text:    validText(event.Content),
		Time:    event.RecordedAt.Time().Format(time.RFC3339),
	}
	switch event.Type {
	case "user_message":
		view.Role = "user"
		view.Label = "You"
	case "resident_message", "outbound_initiative":
		view.Role = "resident"
		view.Label = validText(residentName)
	default:
		return messageView{}, false
	}
	return view, true
}

func (application *handler) postMessage(response http.ResponseWriter, request *http.Request) {
	if !sameOriginMutationWithHTTPS(request, application.options.DialogueAllowRemote) {
		writeError(response, http.StatusForbidden, "cross-origin requests are not accepted")
		return
	}
	ingress, err := decodeMessage(response, request, application.options.MaxMessageBytes)
	if err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	event, err := application.service.IngressWithMetadata(request.Context(), ingress)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidEventReference) {
			writeError(response, http.StatusBadRequest, "event references are invalid")
			return
		}
		application.options.Logger.ErrorContext(request.Context(), "ingress user message", "error", err)
		writeError(response, http.StatusServiceUnavailable, "message could not be accepted")
		return
	}
	if err := application.hub.PublishCommitted(event); err != nil {
		// Canonical ingress already succeeded; an ephemeral notification failure
		// must never turn the write into an apparent failure.
		application.options.Logger.WarnContext(request.Context(), "publish committed user event", "error", err)
	}
	if prefersHTML(request) {
		http.Redirect(response, request, "/", http.StatusSeeOther)
		return
	}
	writeJSON(response, http.StatusCreated, map[string]string{
		"event_id": event.ID.String(),
		"status":   "committed",
	})
}

func decodeMessage(response http.ResponseWriter, request *http.Request, maximum int64) (domain.IngressRequest, error) {
	// Percent encoding can expand each UTF-8 byte to three ASCII bytes.
	request.Body = http.MaxBytesReader(response, request.Body, maximum*3+4096)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		var maximumError *http.MaxBytesError
		if errors.As(err, &maximumError) {
			return domain.IngressRequest{}, errors.New("request body is too large")
		}
		return domain.IngressRequest{}, errors.New("request body could not be read")
	}
	if !utf8.Valid(bodyBytes) {
		return domain.IngressRequest{}, errors.New("message must be valid UTF-8")
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil {
		return domain.IngressRequest{}, errors.New("a valid Content-Type is required")
	}
	var text string
	var rawRefs []string
	switch mediaType {
	case "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(bodyBytes))
		if err != nil {
			return domain.IngressRequest{}, errors.New("invalid form body")
		}
		messages, present := values["message"]
		if !present || len(messages) != 1 {
			return domain.IngressRequest{}, errors.New("form body must contain exactly one message")
		}
		for key := range values {
			if key != "message" && key != "event_ref" {
				return domain.IngressRequest{}, errors.New("form body contains an unknown field")
			}
		}
		text = messages[0]
		rawRefs = values["event_ref"]
	case "application/json":
		decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
		decoder.DisallowUnknownFields()
		var body struct {
			Message   string   `json:"message"`
			EventRefs []string `json:"event_refs"`
		}
		if err := decoder.Decode(&body); err != nil {
			return domain.IngressRequest{}, errors.New("invalid JSON body")
		}
		if err := requireJSONEOF(decoder); err != nil {
			return domain.IngressRequest{}, errors.New("invalid JSON body")
		}
		text = body.Message
		rawRefs = body.EventRefs
	default:
		return domain.IngressRequest{}, fmt.Errorf("unsupported Content-Type %q", mediaType)
	}
	if text == "" || strings.TrimSpace(text) == "" {
		return domain.IngressRequest{}, errors.New("message must not be empty")
	}
	if !utf8.ValidString(text) {
		return domain.IngressRequest{}, errors.New("message must be valid UTF-8")
	}
	if int64(len(text)) > maximum {
		return domain.IngressRequest{}, fmt.Errorf("message exceeds the %d-byte limit", maximum)
	}
	if strings.IndexByte(text, 0) >= 0 {
		return domain.IngressRequest{}, errors.New("message must not contain NUL")
	}
	refs, err := parseExplicitEventRefs(rawRefs)
	if err != nil {
		return domain.IngressRequest{}, err
	}
	return domain.IngressRequest{RawText: text, ExplicitEventIDs: refs}, nil
}

func parseExplicitEventRefs(values []string) ([]canonical.ID, error) {
	if len(values) != 0 {
		return nil, errors.New("event references are invalid")
	}
	return nil, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("additional JSON value")
	}
	return err
}

func sameOrigin(request *http.Request) bool {
	return sameOriginWithHTTPS(request, false)
}

func sameOriginWithHTTPS(request *http.Request, allowHTTPS bool) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	// Remote dialogue may be reached through a TLS-terminating proxy that
	// preserves Host. Forwarded headers never determine the trusted authority.
	schemeAllowed := strings.EqualFold(parsed.Scheme, "http") || allowHTTPS && strings.EqualFold(parsed.Scheme, "https")
	return schemeAllowed && strings.EqualFold(parsed.Host, request.Host)
}

// sameOriginMutation accepts browser mutation requests only when the browser
// proves the request's same-origin capability. Origin is the primary proof;
// the Fetch Metadata fallback covers browser shapes that omit Origin without
// opening the endpoint to an unmarked direct client.
func sameOriginMutation(request *http.Request) bool {
	return sameOriginMutationWithHTTPS(request, false)
}

func sameOriginMutationWithHTTPS(request *http.Request, allowHTTPS bool) bool {
	if request.Header.Get("Origin") != "" {
		return sameOriginWithHTTPS(request, allowHTTPS)
	}
	if !strings.EqualFold(request.Header.Get("Sec-Fetch-Site"), "same-origin") {
		return false
	}
	switch strings.ToLower(request.Header.Get("Sec-Fetch-Mode")) {
	case "cors", "same-origin", "navigate":
		return true
	default:
		return false
	}
}

// sameOriginAudioStream accepts the two browser shapes used for EventSource.
// Some engines omit Origin for a same-origin GET, but Fetch Metadata remains
// browser-controlled and distinguishes it from a hostile cross-site page.
func sameOriginAudioStream(request *http.Request) bool {
	return sameOriginAudioStreamWithHTTPS(request, false)
}

func sameOriginAudioStreamWithHTTPS(request *http.Request, allowHTTPS bool) bool {
	if request.Header.Get("Origin") != "" {
		return sameOriginWithHTTPS(request, allowHTTPS)
	}
	return strings.EqualFold(request.Header.Get("Sec-Fetch-Site"), "same-origin") &&
		strings.EqualFold(request.Header.Get("Sec-Fetch-Mode"), "cors")
}

func prefersHTML(request *http.Request) bool {
	return strings.Contains(request.Header.Get("Accept"), "text/html") &&
		!strings.Contains(request.Header.Get("Accept"), "application/json")
}

func (application *handler) events(response http.ResponseWriter, request *http.Request) {
	flusher, ok := response.(http.Flusher)
	if !ok {
		http.Error(response, "streaming is unavailable", http.StatusInternalServerError)
		return
	}
	capabilities, err := parseEventCapabilities(request.URL.RawQuery)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid event stream capabilities")
		return
	}
	if capabilities.Audio && !sameOriginAudioStreamWithHTTPS(request, application.options.DialogueAllowRemote) {
		writeError(response, http.StatusForbidden, "audio event streams require a same-origin browser")
		return
	}
	subscription := application.hub.SubscribeWithCapabilities(request.Context(), capabilities)
	defer subscription.Close()
	response.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	response.Header().Set("Cache-Control", "no-cache, no-store")
	response.Header().Set("Connection", "keep-alive")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(response, "retry: 2000\n\n")
	flusher.Flush()

	ticker := time.NewTicker(application.options.SSEKeepAlive)
	defer ticker.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-subscription.Events:
			if !open {
				return
			}
			payload, err := event.json()
			if err != nil {
				application.options.Logger.WarnContext(request.Context(), "encode SSE event", "error", err)
				continue
			}
			if _, err := fmt.Fprintf(response, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, payload); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := io.WriteString(response, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func parseEventCapabilities(rawQuery string) (SubscriberCapabilities, error) {
	if rawQuery == "" {
		return SubscriberCapabilities{}, nil
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil || len(values) != 1 {
		return SubscriberCapabilities{}, errors.New("invalid event stream query")
	}
	audio, ok := values["audio"]
	if !ok || len(audio) != 1 || audio[0] != "1" {
		return SubscriberCapabilities{}, errors.New("invalid audio capability")
	}
	return SubscriberCapabilities{Audio: true}, nil
}

func (application *handler) health(response http.ResponseWriter, request *http.Request) {
	result, err := application.healthResult(request.Context())
	if err != nil {
		application.options.Logger.WarnContext(request.Context(), "evaluate health readiness", "error", err)
		result = readiness.Result{ReasonCodes: []readiness.ReasonCode{readiness.ReasonStartupIncomplete}}
	}
	checked := application.options.HealthClock().UnixMicro()
	if checked < 0 {
		checked = 0
	}
	wire, err := healthwire.New(result, strconv.FormatInt(checked, 10))
	if err != nil {
		application.options.Logger.ErrorContext(request.Context(), "construct health response", "error", err)
		wire = healthwire.Response{
			CheckedAtUnixMicros: "0", FormatVersion: healthwire.FormatVersion,
			Ready: false, ReasonCode: readiness.ReasonStartupIncomplete,
		}
	}
	body, err := healthwire.MarshalExact(wire)
	if err != nil {
		// Every value above is closed and validated. Keep this last-resort branch
		// identifier-free if an invariant is accidentally weakened later.
		http.Error(response, "health response unavailable", http.StatusServiceUnavailable)
		return
	}
	status := http.StatusServiceUnavailable
	if wire.Ready {
		status = http.StatusOK
	}
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_, _ = response.Write(body)
}

func (application *handler) healthResult(ctx context.Context) (readiness.Result, error) {
	if application.options.Health != nil {
		return application.options.Health(ctx)
	}
	// Compatibility fallback for embeddings which have not yet supplied the
	// M7 evaluator. Production serve always supplies Options.Health.
	resident, err := application.service.ActiveResident(ctx)
	if err != nil {
		return readiness.Result{ReasonCodes: []readiness.ReasonCode{readiness.ReasonResidentUnselected}}, nil
	}
	if resident.Status != "active" {
		return readiness.Result{ReasonCodes: []readiness.ReasonCode{readiness.ReasonResidentNotActive}}, nil
	}
	return readiness.Result{Ready: true, ReasonCodes: []readiness.ReasonCode{readiness.ReasonReady}}, nil
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func securityHeaders(next http.Handler) http.Handler {
	return dialogueSecurityHeaders(next, false)
}

func dialogueSecurityHeaders(next http.Handler, allowRemote bool) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Security-Policy", "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; media-src 'self' blob:; object-src 'none'; script-src 'self'; style-src 'self'")
		response.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
		response.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
		response.Header().Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
		response.Header().Set("Referrer-Policy", "no-referrer")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("X-Frame-Options", "DENY")
		if !allowRemote && !loopbackRequestHost(request.Host) {
			writeError(response, http.StatusMisdirectedRequest, "request host is not a loopback address")
			return
		}
		next.ServeHTTP(response, request)
	})
}

func loopbackRequestHost(hostPort string) bool {
	if hostPort == "" || strings.TrimSpace(hostPort) != hostPort {
		return false
	}
	host := hostPort
	if parsedHost, _, err := net.SplitHostPort(hostPort); err == nil {
		host = parsedHost
	} else if strings.HasPrefix(hostPort, "[") && strings.HasSuffix(hostPort, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(hostPort, "["), "]")
	} else if strings.Contains(hostPort, ":") {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validText(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	return strings.ToValidUTF8(value, "�")
}
