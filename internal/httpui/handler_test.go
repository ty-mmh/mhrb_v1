package httpui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/readiness"
	"mahoroba.local/mahoroba/internal/tts"
)

type fakeService struct {
	mu           sync.Mutex
	resident     domain.ResidentSnapshot
	activeErr    error
	history      []domain.Event
	historyErr   error
	ingressEvent domain.Event
	ingressErr   error
	gotRequest   domain.IngressRequest
}

func (service *fakeService) IngressWithMetadata(_ context.Context, request domain.IngressRequest) (domain.Event, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.gotRequest = request
	return service.ingressEvent, service.ingressErr
}

func (service *fakeService) History(_ context.Context, residentID canonical.ID, _ int) ([]domain.Event, error) {
	if residentID != service.resident.ResidentID {
		return nil, context.Canceled
	}
	return append([]domain.Event(nil), service.history...), service.historyErr
}

func (service *fakeService) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	return service.resident, service.activeErr
}

func (service *fakeService) message() string {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.gotRequest.RawText
}

func (service *fakeService) request() domain.IngressRequest {
	service.mu.Lock()
	defer service.mu.Unlock()
	result := service.gotRequest
	result.ExplicitEventIDs = append([]canonical.ID(nil), result.ExplicitEventIDs...)
	return result
}

func testID(t *testing.T, value string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func readyService(t *testing.T) *fakeService {
	t.Helper()
	residentID := testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID := testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	return &fakeService{
		resident: domain.ResidentSnapshot{
			ResidentID: residentID,
			Name:       "Haru",
			Status:     "active",
		},
		ingressEvent: domain.Event{
			ID:         eventID,
			ResidentID: residentID,
			Type:       "user_message",
			Content:    "hello",
			RecordedAt: canonical.InstantFromTime(time.Unix(1_700_000_000, 0)),
		},
	}
}

func testHandler(t *testing.T, service Service, hub *Hub, mutate func(*Options)) http.Handler {
	t.Helper()
	options := Options{}
	if mutate != nil {
		mutate(&options)
	}
	server, err := New(service, hub, options)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func TestIndexRendersEscapedCanonicalHistory(t *testing.T) {
	service := readyService(t)
	service.resident.Name = `<script>alert("resident")</script>`
	service.history = []domain.Event{{
		ID:         service.ingressEvent.ID,
		ResidentID: service.resident.ResidentID,
		Type:       "resident_message",
		Content:    `<img src=x onerror="alert(1)">`,
		RecordedAt: canonical.InstantFromTime(time.Unix(1_700_000_000, 0)),
	}}
	handler := testHandler(t, service, NewHub(4), nil)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Host = "127.0.0.1:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body)
	}
	body := response.Body.String()
	if strings.Contains(body, `<script>alert("resident")</script>`) || strings.Contains(body, `<img src=x`) {
		t.Fatalf("unescaped authority content in page: %s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;") || !strings.Contains(body, "&lt;img") {
		t.Fatalf("escaped content missing: %s", body)
	}
	if got := response.Header().Get("Content-Security-Policy"); !strings.Contains(got, "script-src 'self'") || !strings.Contains(got, "media-src 'self' blob:") {
		t.Fatalf("CSP = %q", got)
	}
}

func TestIndexRendersAudioToggleOutboundAndNeverSelfTalk(t *testing.T) {
	service := readyService(t)
	runID := testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	service.history = []domain.Event{
		{ID: testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY"), ResidentID: service.resident.ResidentID, Type: "outbound_initiative", GenerationRunID: &runID, Content: "visible initiative"},
		{ID: testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAZ"), ResidentID: service.resident.ResidentID, Type: "self_talk", GenerationRunID: &runID, Content: "private thought"},
	}
	handler := testHandler(t, service, NewHub(4), func(options *Options) { options.AudioEnabled = true })
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Host = "127.0.0.1:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	if !strings.Contains(body, `id="voice-output"`) || !strings.Contains(body, "visible initiative") || !strings.Contains(body, "message-resident") {
		t.Fatalf("audio toggle or initiative missing: %s", body)
	}
	if strings.Contains(body, "private thought") {
		t.Fatalf("self-talk leaked into history: %s", body)
	}
}

func TestPostMessageCommitsAndPublishes(t *testing.T) {
	service := readyService(t)
	hub := NewHub(4)
	subscription := hub.Subscribe(context.Background())
	defer subscription.Close()
	handler := testHandler(t, service, hub, nil)

	request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"message":"こんにちは"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Origin", "http://127.0.0.1:8787")
	request.Host = "127.0.0.1:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body)
	}
	var accepted map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil ||
		accepted["event_id"] != service.ingressEvent.ID.String() || accepted["status"] != "committed" {
		t.Fatalf("accepted response = %v, error=%v", accepted, err)
	}
	if got := service.message(); got != "こんにちは" {
		t.Fatalf("ingress text = %q", got)
	}
	select {
	case event := <-subscription.Events:
		if event.Type != EventCommitted || event.EventID != service.ingressEvent.ID.String() {
			t.Fatalf("stream event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("committed event was not published")
	}
}

func TestCOV1PostMessageHTMLKeepsSeeOtherAfterCommittedIngress(t *testing.T) {
	service := readyService(t)
	handler := testHandler(t, service, NewHub(2), nil)
	request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader("message=accepted"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "text/html")
	request.Header.Set("Origin", "http://127.0.0.1:8787")
	request.Host = "127.0.0.1:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || service.message() != "accepted" {
		t.Fatalf("HTML accepted response = status %d, message %q", response.Code, service.message())
	}
}

func TestPostMessageRequiresStrictSameOriginCapability(t *testing.T) {
	for _, test := range []struct {
		name      string
		origin    string
		fetchSite string
		fetchMode string
		want      int
	}{
		{name: "exact origin", origin: "http://127.0.0.1:8787", want: http.StatusCreated},
		{name: "missing origin", want: http.StatusForbidden},
		{name: "missing origin with strict fetch metadata", fetchSite: "same-origin", fetchMode: "cors", want: http.StatusCreated},
		{name: "missing origin with invalid fetch mode", fetchSite: "same-origin", fetchMode: "no-cors", want: http.StatusForbidden},
		{name: "cross-site fetch metadata", fetchSite: "cross-site", fetchMode: "cors", want: http.StatusForbidden},
		{name: "cross origin", origin: "https://attacker.example", want: http.StatusForbidden},
		{name: "malformed origin", origin: "http://[::1", want: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := readyService(t)
			handler := testHandler(t, service, NewHub(2), nil)
			request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"message":"ok"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			request.Host = "127.0.0.1:8787"
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.fetchSite != "" {
				request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			}
			if test.fetchMode != "" {
				request.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body)
			}
			if test.want != http.StatusCreated && service.message() != "" {
				t.Fatalf("rejected request reached ingress: %q", service.message())
			}
		})
	}
}

func TestCOV1PostMessageRejectsExplicitEventMetadataBeforeIngress(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{
			name:        "JSON",
			contentType: "application/json",
			body:        `{"message":"literal event:01ARZ3NDEKTSV4RRFFQ69G5FAZ","event_refs":["01ARZ3NDEKTSV4RRFFQ69G5FAX","01ARZ3NDEKTSV4RRFFQ69G5FAY"]}`,
		},
		{
			name:        "form",
			contentType: "application/x-www-form-urlencoded",
			body:        "message=literal+text&event_ref=01ARZ3NDEKTSV4RRFFQ69G5FAX&event_ref=01ARZ3NDEKTSV4RRFFQ69G5FAY",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := readyService(t)
			handler := testHandler(t, service, NewHub(2), nil)
			request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Origin", "http://127.0.0.1:8787")
			request.Host = "127.0.0.1:8787"
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "event references are invalid") {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body)
			}
			if got := service.message(); got != "" {
				t.Fatalf("rejected request reached ingress: %q", got)
			}
		})
	}
}

func TestPostMessageMapsAuthoritativeReferenceRejectionToBadRequest(t *testing.T) {
	service := readyService(t)
	service.ingressErr = domain.ErrInvalidEventReference
	handler := testHandler(t, service, NewHub(2), nil)
	request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(
		`{"message":"ok","event_refs":["01ARZ3NDEKTSV4RRFFQ69G5FAX"]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Origin", "http://127.0.0.1:8787")
	request.Host = "127.0.0.1:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "event references are invalid") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
}

func TestPostMessageRejectsInvalidInputBeforeIngress(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        []byte
		origin      string
		maximum     int64
	}{
		{name: "empty", contentType: "application/json", body: []byte(`{"message":"  "}`)},
		{name: "too long", contentType: "application/json", body: []byte(`{"message":"123456"}`), maximum: 5},
		{name: "invalid UTF-8", contentType: "application/json", body: []byte{'{', '"', 'm', 'e', 's', 's', 'a', 'g', 'e', '"', ':', '"', 0xff, '"', '}'}},
		{name: "unknown JSON field", contentType: "application/json", body: []byte(`{"message":"ok","extra":true}`)},
		{name: "noncanonical event ref", contentType: "application/json", body: []byte(`{"message":"ok","event_refs":["01arz3ndektsv4rrffq69g5fav"]}`)},
		{name: "duplicate event ref", contentType: "application/json", body: []byte(`{"message":"ok","event_refs":["01ARZ3NDEKTSV4RRFFQ69G5FAV","01ARZ3NDEKTSV4RRFFQ69G5FAV"]}`)},
		{name: "too many event refs", contentType: "application/json", body: []byte(`{"message":"ok","event_refs":["01ARZ3NDEKTSV4RRFFQ69G5FAV","01ARZ3NDEKTSV4RRFFQ69G5FAW","01ARZ3NDEKTSV4RRFFQ69G5FAX","01ARZ3NDEKTSV4RRFFQ69G5FAY","01ARZ3NDEKTSV4RRFFQ69G5FAZ"]}`)},
		{name: "duplicate form field", contentType: "application/x-www-form-urlencoded", body: []byte("message=a&message=b")},
		{name: "unknown form field", contentType: "application/x-www-form-urlencoded", body: []byte("message=a&extra=b")},
		{name: "cross origin", contentType: "application/json", body: []byte(`{"message":"ok"}`), origin: "https://attacker.example"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := readyService(t)
			handler := testHandler(t, service, NewHub(2), func(options *Options) {
				if test.maximum != 0 {
					options.MaxMessageBytes = test.maximum
				}
			})
			request := httptest.NewRequest(http.MethodPost, "/messages", bytes.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Accept", "application/json")
			request.Host = "127.0.0.1:8787"
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body)
			}
			if got := service.message(); got != "" {
				t.Fatalf("invalid message reached service: %q", got)
			}
		})
	}
}

func TestM7PostMessageSameOriginBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		origin     string
		fetchSite  string
		fetchMode  string
		wantStatus int
	}{
		{name: "missing origin and metadata", wantStatus: http.StatusForbidden},
		{name: "trusted metadata fallback", fetchSite: "same-origin", fetchMode: "cors", wantStatus: http.StatusCreated},
		{name: "cross-site metadata", fetchSite: "cross-site", fetchMode: "cors", wantStatus: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := readyService(t)
			handler := testHandler(t, service, NewHub(2), nil)
			request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"message":"boundary"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			request.Host = "127.0.0.1:8787"
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.fetchSite != "" {
				request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			}
			if test.fetchMode != "" {
				request.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantStatus == http.StatusForbidden && service.message() != "" {
				t.Fatal("rejected browser mutation reached message ingress")
			}
		})
	}
}

func TestUnavailableIndexAndHealth(t *testing.T) {
	service := readyService(t)
	service.resident.Status = "draft"
	handler := testHandler(t, service, NewHub(2), nil)
	for _, path := range []string{"/", "/healthz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Host = "127.0.0.1:8787"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status = %d", path, response.Code)
		}
	}
}

func TestM7HealthHandlerEmitsExactVersionedReadinessWire(t *testing.T) {
	service := readyService(t)
	fixed := time.Unix(1_787_220_000, 0)
	tests := []struct {
		name       string
		result     readiness.Result
		wantStatus int
		wantBody   string
	}{
		{
			name: "ready", result: readiness.Result{Ready: true, ReasonCodes: []readiness.ReasonCode{readiness.ReasonReady}},
			wantStatus: http.StatusOK,
			wantBody:   `{"checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":true,"reason_code":"ready"}` + "\n",
		},
		{
			name: "priority reason", result: readiness.Result{ReasonCodes: []readiness.ReasonCode{readiness.ReasonIntegrityBlocked, readiness.ReasonShutdown}},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"checked_at_unix_micros":"1787220000000000","format_version":"mahoroba-health-v1","ready":false,"reason_code":"integrity_readiness_blocked"}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := testHandler(t, service, NewHub(2), func(options *Options) {
				options.Health = func(context.Context) (readiness.Result, error) { return test.result, nil }
				options.HealthClock = func() time.Time { return fixed }
			})
			request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			request.Host = "127.0.0.1:8787"
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus || response.Body.String() != test.wantBody {
				t.Fatalf("status/body = %d %q, want %d %q", response.Code, response.Body.String(), test.wantStatus, test.wantBody)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q", got)
			}
		})
	}
}

func TestEventsStreamsJSONSSE(t *testing.T) {
	service := readyService(t)
	hub := NewHub(4)
	handler := testHandler(t, service, hub, func(options *Options) {
		options.SSEKeepAlive = time.Second
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "retry: 2000\n" {
		t.Fatalf("retry line = %q, err=%v", line, err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	waitForSubscriber(t, hub)
	if err := hub.Publish(StreamEvent{Type: EventStatus, Status: "ready", Message: "ok"}); err != nil {
		t.Fatal(err)
	}

	var lines []string
	for len(lines) < 4 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	joined := strings.Join(lines, "")
	if !strings.Contains(joined, "event: status\n") || !strings.Contains(joined, `"message":"ok"`) {
		t.Fatalf("SSE response = %q", joined)
	}
}

func TestAudioEventStreamRequiresStrictSameOriginCapability(t *testing.T) {
	handler := testHandler(t, readyService(t), NewHub(2), nil)
	for _, test := range []struct {
		name, target, origin string
		want                 int
	}{
		{name: "missing origin", target: "/events?audio=1", want: http.StatusForbidden},
		{name: "cross origin", target: "/events?audio=1", origin: "http://example.test", want: http.StatusForbidden},
		{name: "cross-site fetch metadata", target: "/events?audio=1", want: http.StatusForbidden},
		{name: "unknown capability", target: "/events?voice=1", want: http.StatusBadRequest},
		{name: "duplicate capability", target: "/events?audio=1&audio=1", origin: "http://127.0.0.1:8787", want: http.StatusBadRequest},
		{name: "disabled value", target: "/events?audio=0", want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.target, nil)
			request.Host = "127.0.0.1:8787"
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.name == "cross-site fetch metadata" {
				request.Header.Set("Sec-Fetch-Site", "cross-site")
				request.Header.Set("Sec-Fetch-Mode", "cors")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestAudioEventStreamPublishesBase64OnlyToCapableConnection(t *testing.T) {
	service := readyService(t)
	hub := NewHub(4)
	handler := testHandler(t, service, hub, func(options *Options) { options.SSEKeepAlive = time.Second })
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/events?audio=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Chromium's same-origin EventSource omits Origin and instead supplies
	// browser-controlled Fetch Metadata headers.
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.Header.Set("Sec-Fetch-Mode", "cors")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	waitForSubscriber(t, hub)
	if !hub.HasAudioSubscribers() {
		t.Fatal("audio connection did not register demand")
	}
	runID := testID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err := hub.PublishAudio(tts.Delivery{
		ResidentID: service.resident.ResidentID, SourceEventID: service.ingressEvent.ID,
		GenerationRunID: runID,
		Audio:           tts.Audio{MIMEType: tts.MIMETypeWAV, Data: hubTestWAV()},
	}); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for len(lines) < 4 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	joined := strings.Join(lines, "")
	if !strings.Contains(joined, "event: audio\n") || !strings.Contains(joined, `"mime_type":"audio/wav"`) || !strings.Contains(joined, `"audio_base64":`) {
		t.Fatalf("audio SSE = %q", joined)
	}
}

func waitForSubscriber(t *testing.T, hub *Hub) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		count := len(hub.subscribers)
		hub.mu.Unlock()
		if count > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("SSE handler did not subscribe")
}

func TestStaticAssetsAreEmbedded(t *testing.T) {
	handler := testHandler(t, readyService(t), NewHub(2), nil)
	for _, asset := range []string{"/static/app.js", "/static/app.css"} {
		request := httptest.NewRequest(http.MethodGet, asset, nil)
		request.Host = "127.0.0.1:8787"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d", asset, response.Code)
		}
		body, _ := io.ReadAll(response.Result().Body)
		if len(body) < 100 {
			t.Fatalf("%s body is unexpectedly short", asset)
		}
	}
}

func TestUnknownPathsAndStaticDirectoryReturnNotFound(t *testing.T) {
	handler := testHandler(t, readyService(t), NewHub(2), nil)
	for _, path := range []string{"/unknown", "/static/"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Host = "127.0.0.1:8787"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d", path, response.Code)
		}
	}
}

func TestRejectsNonLoopbackHost(t *testing.T) {
	handler := testHandler(t, readyService(t), NewHub(2), nil)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Host = "rebinding.attacker.example:8787"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d", response.Code)
	}
}
