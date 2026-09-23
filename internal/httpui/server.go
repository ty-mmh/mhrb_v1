package httpui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/readiness"
)

const (
	defaultAddress         = "127.0.0.1:8787"
	defaultMaxMessageBytes = 64 * 1024
	defaultHistoryLimit    = 100
)

type Options struct {
	Address           string
	BaseContext       context.Context
	AudioEnabled      bool
	MaxMessageBytes   int64
	HistoryLimit      int
	SSEKeepAlive      time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
	Logger            *slog.Logger
	// DialogueAllowRemote explicitly relaxes only the dialogue server's
	// loopback listener/Host restriction. NewAdmin rejects this option.
	DialogueAllowRemote bool
	// ContainerListen permits a non-loopback container listener while retaining
	// loopback Host and same-origin checks. Publish its ports on host loopback only.
	ContainerListen bool
	// ManagementURL and DialogueURL link the separate local UI entry points.
	ManagementURL     string
	DialogueURL       string
	ManagementDataDir string
	// Health evaluates the same captured-head readiness contract used by
	// startup, restore, and diagnostics. It is called once per /healthz request.
	Health      func(context.Context) (readiness.Result, error)
	HealthClock func() time.Time
}

func (options Options) withDefaults() Options {
	if options.Address == "" {
		options.Address = defaultAddress
	}
	if options.MaxMessageBytes == 0 {
		options.MaxMessageBytes = defaultMaxMessageBytes
	}
	if options.HistoryLimit == 0 {
		options.HistoryLimit = defaultHistoryLimit
	}
	if options.SSEKeepAlive == 0 {
		options.SSEKeepAlive = 15 * time.Second
	}
	if options.ReadHeaderTimeout == 0 {
		options.ReadHeaderTimeout = 5 * time.Second
	}
	if options.ShutdownTimeout == 0 {
		options.ShutdownTimeout = 10 * time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if options.HealthClock == nil {
		options.HealthClock = time.Now
	}
	return options
}

func (options Options) validate() (Options, error) {
	options = options.withDefaults()
	if options.MaxMessageBytes < 1 || options.MaxMessageBytes > 1024*1024 {
		return Options{}, fmt.Errorf("httpui: max message bytes must be in 1..1048576")
	}
	if options.HistoryLimit < 1 || options.HistoryLimit > 1000 {
		return Options{}, fmt.Errorf("httpui: history limit must be in 1..1000")
	}
	if options.SSEKeepAlive < time.Second {
		return Options{}, fmt.Errorf("httpui: SSE keepalive must be at least one second")
	}
	if options.ReadHeaderTimeout <= 0 || options.ShutdownTimeout <= 0 {
		return Options{}, fmt.Errorf("httpui: server timeouts must be positive")
	}
	normalized, err := normalizeDialogueAddress(options.Address, options.DialogueAllowRemote || options.ContainerListen)
	if err != nil {
		return Options{}, err
	}
	options.Address = normalized
	return options, nil
}

// Server owns the listener and HTTP handler. Handler is exposed for
// httptest and embedding, but production callers should use ListenAndServe or
// Serve so the configured listener boundary is enforced at the socket.
type Server struct {
	options Options
	hub     *Hub
	http    *http.Server
}

func New(service Service, hub *Hub, options Options) (*Server, error) {
	if service == nil {
		return nil, errors.New("httpui: nil service")
	}
	validated, err := options.validate()
	if err != nil {
		return nil, err
	}
	if hub == nil {
		hub = NewHub(64)
	}
	handler, err := newHandler(service, hub, validated)
	if err != nil {
		return nil, err
	}
	return newServer(handler, hub, validated), nil
}

func newServer(handler http.Handler, hub *Hub, options Options) *Server {
	server := &Server{options: options, hub: hub}
	baseContext := options.BaseContext
	if baseContext == nil {
		baseContext = context.Background()
	}
	server.http = &http.Server{
		Addr:    options.Address,
		Handler: handler,
		BaseContext: func(net.Listener) context.Context {
			return baseContext
		},
		ReadHeaderTimeout: options.ReadHeaderTimeout,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 * 1024,
	}
	return server
}

func (server *Server) Handler() http.Handler { return server.http.Handler }
func (server *Server) Hub() *Hub             { return server.hub }
func (server *Server) Address() string       { return server.options.Address }

func (server *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", server.options.Address)
	if err != nil {
		return fmt.Errorf("httpui: listen on %s: %w", server.options.Address, err)
	}
	return server.Serve(listener)
}

func (server *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("httpui: nil listener")
	}
	if err := verifyDialogueListener(listener, server.options.DialogueAllowRemote || server.options.ContainerListen); err != nil {
		_ = listener.Close()
		return err
	}
	err := server.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (server *Server) Shutdown(ctx context.Context) error {
	// SSE handlers intentionally stay open for the lifetime of their Hub
	// subscription. Close every subscription before asking net/http to drain
	// handlers, otherwise a healthy idle browser can consume the entire graceful
	// shutdown deadline.
	server.hub.Close()
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), server.options.ShutdownTimeout)
		defer cancel()
	}
	return server.http.Shutdown(ctx)
}

func normalizeLoopbackAddress(address string) (string, error) {
	return normalizeDialogueAddress(address, false)
}

func normalizeDialogueAddress(address string, allowRemote bool) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("httpui: address must include host and port: %w", err)
	}
	if port == "" {
		return "", errors.New("httpui: address has an empty port")
	}
	if numeric, err := strconv.ParseUint(port, 10, 16); err != nil || numeric > 65535 {
		return "", fmt.Errorf("httpui: invalid port %q", port)
	}
	if strings.EqualFold(host, "localhost") {
		return net.JoinHostPort("127.0.0.1", port), nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !allowRemote && !ip.IsLoopback() {
		if allowRemote {
			return "", fmt.Errorf("httpui: dialogue listen address must use an IP literal or localhost: %q", address)
		}
		return "", fmt.Errorf("httpui: refusing non-loopback address %q", address)
	}
	return net.JoinHostPort(ip.String(), port), nil
}

func verifyLoopbackListener(listener net.Listener) error {
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.IP == nil || !tcpAddress.IP.IsLoopback() {
		return fmt.Errorf("httpui: refusing non-loopback listener %q", listener.Addr())
	}
	return nil
}

func verifyDialogueListener(listener net.Listener, allowRemote bool) error {
	if !allowRemote {
		return verifyLoopbackListener(listener)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.IP.To16() == nil {
		return fmt.Errorf("httpui: refusing non-TCP dialogue listener %q", listener.Addr())
	}
	return nil
}
