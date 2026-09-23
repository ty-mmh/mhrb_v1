package httpui

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDialogueRemoteHostRequiresExplicitOptIn(t *testing.T) {
	for _, allowRemote := range []bool{false, true} {
		for _, host := range []string{"chat.example.test:8787", "100.101.102.103:8787", "192.168.1.20:8787", "[2001:db8::1]:8787"} {
			service := readyService(t)
			server, err := New(service, nil, Options{DialogueAllowRemote: allowRemote})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Host = host
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			want := http.StatusMisdirectedRequest
			if allowRemote {
				want = http.StatusOK
			}
			if response.Code != want {
				t.Fatalf("allowRemote=%v host=%q status=%d body=%s", allowRemote, host, response.Code, response.Body)
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "" ||
				!strings.Contains(response.Header().Get("Content-Security-Policy"), "script-src 'self'") {
				t.Fatalf("remote option changed CORS/CSP: %v", response.Header())
			}
		}
	}
}

func TestRemoteDialogueRetainsSameOriginMutations(t *testing.T) {
	for _, test := range []struct {
		name        string
		allowRemote bool
		host        string
		origin      string
		fetchSite   string
		fetchMode   string
		forwarded   bool
		want        int
	}{
		{name: "HTTP custom host", allowRemote: true, host: "chat.example.test:8787", origin: "http://chat.example.test:8787", want: http.StatusCreated},
		{name: "HTTP Tailscale IP", allowRemote: true, host: "100.101.102.103:8787", origin: "http://100.101.102.103:8787", want: http.StatusCreated},
		{name: "HTTPS preserved proxy host", allowRemote: true, host: "chat.example.test", origin: "https://chat.example.test", want: http.StatusCreated},
		{name: "HTTPS custom port", allowRemote: true, host: "chat.example.test:8443", origin: "https://chat.example.test:8443", want: http.StatusCreated},
		{name: "foreign origin", allowRemote: true, host: "chat.example.test", origin: "https://attacker.example", want: http.StatusForbidden},
		{name: "different origin port", allowRemote: true, host: "chat.example.test:8787", origin: "http://chat.example.test:8080", want: http.StatusForbidden},
		{name: "unsupported origin scheme", allowRemote: true, host: "chat.example.test", origin: "ftp://chat.example.test", want: http.StatusForbidden},
		{name: "invalid origin path", allowRemote: true, host: "chat.example.test", origin: "https://chat.example.test/path", want: http.StatusForbidden},
		{name: "missing origin and metadata", allowRemote: true, host: "chat.example.test", want: http.StatusForbidden},
		{name: "same origin metadata", allowRemote: true, host: "chat.example.test", fetchSite: "same-origin", fetchMode: "cors", want: http.StatusCreated},
		{name: "cross site metadata", allowRemote: true, host: "chat.example.test", fetchSite: "cross-site", fetchMode: "cors", want: http.StatusForbidden},
		{name: "Forwarded cannot replace Host", allowRemote: true, host: "127.0.0.1:8787", origin: "https://chat.example.test", forwarded: true, want: http.StatusForbidden},
		{name: "default still rejects HTTPS", host: "127.0.0.1:8787", origin: "https://127.0.0.1:8787", want: http.StatusForbidden},
		{name: "default still rejects remote Host", host: "chat.example.test", origin: "http://chat.example.test", want: http.StatusMisdirectedRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := readyService(t)
			server, err := New(service, nil, Options{DialogueAllowRemote: test.allowRemote})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(`{"message":"hello"}`))
			request.Host = test.host
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			request.Header.Set("Sec-Fetch-Mode", test.fetchMode)
			if test.forwarded {
				request.Header.Set("Forwarded", `host=chat.example.test;proto=https`)
				request.Header.Set("X-Forwarded-Host", "chat.example.test")
				request.Header.Set("X-Forwarded-Proto", "https")
			}
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body)
			}
			if (service.message() != "") != (test.want == http.StatusCreated) {
				t.Fatalf("unexpected ingress outcome: %q", service.message())
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("dialogue remote option enabled CORS")
			}
		})
	}
}

func TestRemoteDialogueAudioRetainsSameOriginBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		origin string
		want   int
	}{
		{name: "same HTTPS origin", origin: "https://chat.example.test", want: http.StatusOK},
		{name: "same HTTP origin", origin: "http://chat.example.test", want: http.StatusOK},
		{name: "foreign HTTPS origin", origin: "https://attacker.example", want: http.StatusForbidden},
		{name: "missing origin", want: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			hub := NewHub(1)
			// A closed hub lets an accepted SSE handler send its initial response
			// and return without opening a socket or blocking this test.
			hub.Close()
			server, err := New(readyService(t), hub, Options{DialogueAllowRemote: true, AudioEnabled: true})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "/events?audio=1", nil)
			request.Host = "chat.example.test"
			request.Header.Set("Origin", test.origin)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%s", response.Code, response.Body)
			}
		})
	}
}

func TestDialogueRemoteListenRequiresOptInAndAnIP(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8787", "100.101.102.103:8787", "192.168.1.20:8787", "[::]:8787", "[2001:db8::1]:8787"} {
		if _, err := New(readyService(t), nil, Options{Address: address}); err == nil {
			t.Fatalf("default accepted %q", address)
		}
		server, err := New(readyService(t), nil, Options{Address: address, DialogueAllowRemote: true})
		if err != nil || server.Address() != address {
			t.Fatalf("explicit remote address=%q server=%v err=%v", address, server, err)
		}
	}
	for _, address := range []string{"chat.example.test:8787", ":8787", "0.0.0.0", "0.0.0.0:65536"} {
		if _, err := New(readyService(t), nil, Options{Address: address, DialogueAllowRemote: true}); err == nil {
			t.Fatalf("remote dialogue accepted invalid listen address %q", address)
		}
	}
}

type remoteBoundaryListener struct {
	address  net.Addr
	accepted bool
	closed   bool
}

var errRemoteBoundaryAccept = errors.New("test accept reached")

func (listener *remoteBoundaryListener) Accept() (net.Conn, error) {
	listener.accepted = true
	return nil, errRemoteBoundaryAccept
}

func (listener *remoteBoundaryListener) Close() error {
	listener.closed = true
	return nil
}

func (listener *remoteBoundaryListener) Addr() net.Addr { return listener.address }

func TestDialogueRemoteListenerBoundaryAndAdminIsolation(t *testing.T) {
	for _, allowRemote := range []bool{false, true} {
		server, err := New(readyService(t), nil, Options{DialogueAllowRemote: allowRemote})
		if err != nil {
			t.Fatal(err)
		}
		listener := &remoteBoundaryListener{address: &net.TCPAddr{IP: net.ParseIP("192.168.1.20"), Port: 8787}}
		err = server.Serve(listener)
		if listener.accepted != allowRemote || !listener.closed || (errors.Is(err, errRemoteBoundaryAccept) != allowRemote) {
			t.Fatalf("allowRemote=%v accepted=%v closed=%v err=%v", allowRemote, listener.accepted, listener.closed, err)
		}
	}
	for _, address := range []string{"127.0.0.1:8788", "0.0.0.0:8788"} {
		if _, err := NewAdmin(&fakeAdminService{}, Options{Address: address, DialogueAllowRemote: true}); err == nil {
			t.Fatalf("management accepted dialogue remote option with %q", address)
		}
	}
	server, err := NewAdmin(&fakeAdminService{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener := &remoteBoundaryListener{address: &net.TCPAddr{IP: net.ParseIP("192.168.1.20"), Port: 8788}}
	if err := server.Serve(listener); err == nil || listener.accepted || !listener.closed {
		t.Fatalf("management listener boundary failed: accepted=%v closed=%v err=%v", listener.accepted, listener.closed, err)
	}
	remoteServer, err := New(readyService(t), nil, Options{DialogueAllowRemote: true})
	if err != nil {
		t.Fatal(err)
	}
	unixListener := &remoteBoundaryListener{address: &net.UnixAddr{Name: "not-a-TCP-listener", Net: "unix"}}
	if err := remoteServer.Serve(unixListener); err == nil || unixListener.accepted || !unixListener.closed {
		t.Fatal("remote dialogue accepted a non-TCP listener")
	}
}

func TestAdminKeepsHostAndHTTPSOriginRestrictionsAfterDialogueOptIn(t *testing.T) {
	// Constructing a remote dialogue server must not alter any shared/global
	// policy later used to construct a management server.
	if _, err := New(readyService(t), nil, Options{DialogueAllowRemote: true}); err != nil {
		t.Fatal(err)
	}
	service := &fakeAdminService{}
	server, err := NewAdmin(service, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		host   string
		origin string
		want   int
	}{
		{host: "admin.example.test", origin: "http://admin.example.test", want: http.StatusMisdirectedRequest},
		{host: "127.0.0.1:8788", origin: "https://127.0.0.1:8788", want: http.StatusForbidden},
	} {
		request := httptest.NewRequest(http.MethodPost, "/admin/execute", strings.NewReader(`{"command":"inspect"}`))
		request.Host = test.host
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", test.origin)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != test.want || service.calls != 0 {
			t.Fatalf("management host=%q status=%d calls=%d", test.host, response.Code, service.calls)
		}
	}
}
