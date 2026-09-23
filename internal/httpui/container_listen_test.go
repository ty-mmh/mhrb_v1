package httpui

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestContainerListenRequiresOptInAndKeepsRequestBoundaries(t *testing.T) {
	for _, admin := range []bool{false, true} {
		create := func(options Options) (*Server, error) {
			if admin {
				return NewAdmin(&fakeAdminService{}, options)
			}
			return New(readyService(t), nil, options)
		}
		for _, address := range []string{"0.0.0.0:9876", "[::]:9876", "192.0.2.10:9876"} {
			if _, err := create(Options{Address: address}); err == nil {
				t.Fatalf("admin=%v default accepted %s", admin, address)
			}
			if _, err := create(Options{Address: address, ContainerListen: true}); err != nil {
				t.Fatalf("admin=%v container address %s: %v", admin, address, err)
			}
		}
		for _, address := range []string{"container:9876", ":9876", "0.0.0.0:65536"} {
			if _, err := create(Options{Address: address, ContainerListen: true}); err == nil {
				t.Fatalf("admin=%v accepted invalid address %s", admin, address)
			}
		}
		server, err := create(Options{Address: "0.0.0.0:9876", ContainerListen: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			host, origin string
			want         int
		}{
			{"localhost:9876", "http://localhost:9876", http.StatusOK},
			{"127.0.0.1:9876", "http://127.0.0.1:9876", http.StatusOK},
			{"127.0.0.1:9876", "http://127.0.0.1:9877", http.StatusForbidden},
			{"127.0.0.1:9876", "https://127.0.0.1:9876", http.StatusForbidden},
			{"127.0.0.1:9876", "http://foreign.example", http.StatusForbidden},
			{"127.0.0.1:9876", "", http.StatusForbidden},
			{"container:9876", "http://container:9876", http.StatusMisdirectedRequest},
			{"192.0.2.10:9876", "http://192.0.2.10:9876", http.StatusMisdirectedRequest},
		} {
			path, body, want := "/admin/execute", `{"command":"inspect"}`, test.want
			if !admin {
				path, body = "/messages", `{"message":"hello"}`
				if want == http.StatusOK {
					want = http.StatusCreated
				}
			}
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			request.Host = test.host
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Forwarded", "host=localhost:9876;proto=http")
			request.Header.Set("X-Forwarded-Host", "localhost:9876")
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != want {
				t.Fatalf("admin=%v host=%s origin=%s: got %d want %d: %s", admin, test.host, test.origin, response.Code, want, response.Body)
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "" || !strings.Contains(response.Header().Get("Content-Security-Policy"), "script-src 'self'") {
				t.Fatal("container binding changed CORS or CSP")
			}
		}
		listener := &remoteBoundaryListener{address: &net.TCPAddr{IP: net.IPv4zero, Port: 9876}}
		if err := server.Serve(listener); !errors.Is(err, errRemoteBoundaryAccept) || !listener.accepted || !listener.closed {
			t.Fatalf("admin=%v container listener: %v", admin, err)
		}
		invalid, err := create(Options{ContainerListen: true})
		if err != nil {
			t.Fatal(err)
		}
		unix := &remoteBoundaryListener{address: &net.UnixAddr{Name: "not-tcp", Net: "unix"}}
		if err := invalid.Serve(unix); err == nil || unix.accepted || !unix.closed {
			t.Fatal("container option accepted non-TCP listener")
		}
	}
	if _, err := NewAdmin(&fakeAdminService{}, Options{ContainerListen: true, DialogueAllowRemote: true}); err == nil {
		t.Fatal("container binding enabled remote management Hosts")
	}
}

func TestAdminHealthIsLivenessWithoutDialogueAndRetainsHostGuard(t *testing.T) {
	service := &fakeAdminService{}
	server, err := NewAdmin(service, Options{ContainerListen: true, Address: "0.0.0.0:9876"})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"127.0.0.1:9876", "localhost:9876", "admin.example:9876"} {
		request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		request.Host = host
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if host == "admin.example:9876" {
			if response.Code != http.StatusMisdirectedRequest {
				t.Fatal("health bypassed Host guard")
			}
		} else if response.Code != http.StatusOK || response.Body.String() != AdminHealthBody || response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("health = %d %s", response.Code, response.Body)
		}
	}
	if service.calls != 0 {
		t.Fatal("health executed a data command")
	}
}
