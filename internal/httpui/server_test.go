package httpui

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestNormalizeLoopbackAddress(t *testing.T) {
	tests := []struct {
		input string
		want  string
		ok    bool
	}{
		{"localhost:8787", "127.0.0.1:8787", true},
		{"127.0.0.1:0", "127.0.0.1:0", true},
		{"[::1]:9000", "[::1]:9000", true},
		{"0.0.0.0:8787", "", false},
		{"192.168.1.2:8787", "", false},
		{"example.com:8787", "", false},
		{"127.0.0.1", "", false},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := normalizeLoopbackAddress(test.input)
			if (err == nil) != test.ok {
				t.Fatalf("error = %v", err)
			}
			if got != test.want {
				t.Fatalf("normalized = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	service := readyService(t)
	for _, options := range []Options{
		{Address: "0.0.0.0:8787"},
		{MaxMessageBytes: -1},
		{HistoryLimit: 1001},
		{SSEKeepAlive: timeNanosecond},
	} {
		if _, err := New(service, NewHub(1), options); err == nil {
			t.Fatalf("New accepted options %#v", options)
		}
	}
}

func TestShutdownClosesOpenSSESubscriptionBeforeDraining(t *testing.T) {
	hub := NewHub(2)
	server, err := New(readyService(t), hub, Options{
		Address: "127.0.0.1:0", SSEKeepAlive: time.Hour, ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	response, err := http.Get("http://" + listener.Addr().String() + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "retry: 2000\n" {
		t.Fatalf("initial SSE line = %q, %v", line, err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != "\n" {
		t.Fatalf("initial SSE separator = %q, %v", line, err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("SSE body after Shutdown = %v, want EOF", err)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}

type blockingIngressService struct {
	*fakeService
	started  chan struct{}
	finished chan struct{}
}

func (service *blockingIngressService) IngressWithMetadata(ctx context.Context, _ domain.IngressRequest) (domain.Event, error) {
	close(service.started)
	<-ctx.Done()
	close(service.finished)
	return domain.Event{}, ctx.Err()
}

func TestBaseContextCancellationDrainsInFlightPOSTBeforeShutdown(t *testing.T) {
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	service := &blockingIngressService{
		fakeService: readyService(t), started: make(chan struct{}), finished: make(chan struct{}),
	}
	server, err := New(service, NewHub(2), Options{
		Address: "127.0.0.1:0", BaseContext: baseCtx, ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	type requestResult struct {
		response *http.Response
		err      error
	}
	requestDone := make(chan requestResult, 1)
	go func() {
		request, requestErr := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/messages",
			strings.NewReader(`{"message":"wait for shutdown"}`))
		if requestErr != nil {
			requestDone <- requestResult{err: requestErr}
			return
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "http://"+listener.Addr().String())
		response, requestErr := http.DefaultClient.Do(request)
		requestDone <- requestResult{response: response, err: requestErr}
	}()

	select {
	case <-service.started:
	case <-time.After(time.Second):
		t.Fatal("POST handler did not enter ingress")
	}
	cancelBase()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown with cancelled BaseContext: %v", err)
	}
	select {
	case <-service.finished:
	default:
		t.Fatal("Shutdown returned before the in-flight POST observed BaseContext cancellation")
	}
	result := <-requestDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.response.Body.Close()
	if result.response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cancelled POST status = %d, want %d", result.response.StatusCode, http.StatusServiceUnavailable)
	}
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

type stubbornIngressService struct {
	*fakeService
	started chan struct{}
	release chan struct{}
}

func (service *stubbornIngressService) IngressWithMetadata(context.Context, domain.IngressRequest) (domain.Event, error) {
	close(service.started)
	<-service.release
	return service.ingressEvent, nil
}

func TestShutdownDeadlineReportsNonSSEPOSTThatIgnoresCancellation(t *testing.T) {
	service := &stubbornIngressService{
		fakeService: readyService(t), started: make(chan struct{}), release: make(chan struct{}),
	}
	server, err := New(service, NewHub(2), Options{
		Address: "127.0.0.1:0", ShutdownTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	requestDone := make(chan error, 1)
	go func() {
		request, requestErr := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/messages",
			strings.NewReader(`{"message":"ignore cancellation"}`))
		if requestErr != nil {
			requestDone <- requestErr
			return
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", "http://"+listener.Addr().String())
		response, requestErr := http.DefaultClient.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-service.started:
	case <-time.After(time.Second):
		t.Fatal("POST handler did not enter ingress")
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		close(service.release)
		t.Fatalf("Shutdown error = %v, want context deadline exceeded", err)
	}
	select {
	case err := <-requestDone:
		close(service.release)
		t.Fatalf("POST unexpectedly finished before release: %v", err)
	default:
	}
	close(service.release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

const timeNanosecond = 1
