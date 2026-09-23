package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/configsecure"
	"mahoroba.local/mahoroba/internal/healthwire"
	"mahoroba.local/mahoroba/internal/secret"
)

const (
	healthcheckConnectTimeout = time.Second
	healthcheckTotalTimeout   = 3 * time.Second
	healthcheckMaximumBody    = 4096
)

var (
	errHealthcheckRedirect = errors.New("healthcheck: redirect refused")
	errHealthUnreachable   = errors.New("healthcheck: endpoint unreachable")
	errHealthInvalid       = errors.New("healthcheck: invalid response")
)

func runHealthcheck(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "TOML configuration path")
	explicitURL := flags.String("url", "", "loopback health URL")
	if duplicateHealthcheckFlag(arguments) || flags.Parse(arguments) != nil || flags.NArg() != 0 {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandHealthcheck, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	configPresent := healthcheckFlagPresent(arguments, "--config")
	urlPresent := healthcheckFlagPresent(arguments, "--url")
	if configPresent && *configPath == "" || urlPresent && *explicitURL == "" {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandHealthcheck, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}

	target := *explicitURL
	if !urlPresent || configPresent {
		cfg, err := config.Load(config.Overrides{ConfigPath: *configPath})
		if err != nil {
			return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
				cliresult.CommandHealthcheck, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
			))
		}
		if !urlPresent {
			target = dialogueConnectURL(cfg.Server.Listen) + "/healthz"
		}
	}
	normalized, address, err := normalizeHealthcheckURL(target)
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandHealthcheck, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight, nil,
		))
	}

	observation, err := requestHealthcheck(ctx, normalized, newHealthcheckClient(address))
	if err != nil {
		code := cliresult.ErrorHealthcheckInvalidResponse
		if errors.Is(err, errHealthUnreachable) {
			code = cliresult.ErrorHealthcheckUnreachable
		}
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandHealthcheck, code, cliresult.StagePreflight, nil))
	}
	checkedAt := observation.CheckedAtUnixMicros
	reason := string(observation.ReasonCode)
	if !observation.Ready {
		ready := false
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandHealthcheck, cliresult.ErrorHealthcheckNotReady, cliresult.StageReadiness,
			&cliresult.ErrorResult{
				ErrorStage: cliresult.StageReadiness, Ready: &ready,
				ReasonCode: &reason, CheckedAtUnixMicros: &checkedAt,
			},
		))
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewSuccess(
		cliresult.CommandHealthcheck,
		&cliresult.HealthcheckResult{Ready: true, ReasonCode: reason, CheckedAtUnixMicros: checkedAt},
	))
}

func classifyHealthcheckConfigError(err error) cliresult.ErrorCode {
	switch {
	case errors.Is(err, secret.ErrSourceConflict):
		return cliresult.ErrorSecretSourceConflict
	case errors.Is(err, secret.ErrFileUnsupported):
		return cliresult.ErrorSecretFileUnsupportedPlatform
	case errors.Is(err, secret.ErrFileInvalid), errors.Is(err, secret.ErrDirectValueInvalid),
		errors.Is(err, configsecure.ErrInvalid):
		return cliresult.ErrorSecretFileInvalid
	default:
		return cliresult.ErrorConfigurationInvalid
	}
}

func duplicateHealthcheckFlag(arguments []string) bool {
	seen := make(map[string]bool, 2)
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		for _, name := range []string{"--config", "--url"} {
			if argument == name || strings.HasPrefix(argument, name+"=") {
				if seen[name] {
					return true
				}
				seen[name] = true
			}
		}
	}
	return false
}

func healthcheckFlagPresent(arguments []string, name string) bool {
	for _, argument := range arguments {
		if argument == "--" {
			return false
		}
		if argument == name || strings.HasPrefix(argument, name+"=") {
			return true
		}
	}
	return false
}

func normalizeHealthcheckURL(raw string) (string, string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "http" || parsed.Opaque != "" || parsed.User != nil ||
		parsed.Path != "/healthz" || parsed.RawPath != "" || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" {
		return "", "", errors.New("healthcheck: URL is not the exact loopback health endpoint")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil || host == "" || port == "" {
		return "", "", errors.New("healthcheck: URL must contain a loopback host and port")
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return "", "", errors.New("healthcheck: URL port is invalid")
	}
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	} else {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return "", "", errors.New("healthcheck: URL destination is not loopback")
		}
		host = ip.String()
	}
	address := net.JoinHostPort(host, strconv.FormatUint(portNumber, 10))
	return "http://" + address + "/healthz", address, nil
}

func newHealthcheckClient(address string) *http.Client {
	dialer := &net.Dialer{Timeout: healthcheckConnectTimeout}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: healthcheckTotalTimeout,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   healthcheckTotalTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errHealthcheckRedirect
		},
	}
}

func requestHealthcheck(ctx context.Context, endpoint string, client *http.Client) (healthwire.Response, error) {
	if ctx == nil || client == nil {
		return healthwire.Response{}, fmt.Errorf("%w: invalid client", errHealthUnreachable)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return healthwire.Response{}, fmt.Errorf("%w: invalid request", errHealthInvalid)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, errHealthcheckRedirect) {
			return healthwire.Response{}, fmt.Errorf("%w: redirect", errHealthInvalid)
		}
		return healthwire.Response{}, fmt.Errorf("%w: request", errHealthUnreachable)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return healthwire.Response{}, fmt.Errorf("%w: status", errHealthInvalid)
	}
	mediaType, parameters, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || len(parameters) != 0 {
		return healthwire.Response{}, fmt.Errorf("%w: content type", errHealthInvalid)
	}
	if response.ContentLength > healthcheckMaximumBody {
		return healthwire.Response{}, fmt.Errorf("%w: body too large", errHealthInvalid)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, healthcheckMaximumBody+1))
	if err != nil {
		return healthwire.Response{}, fmt.Errorf("%w: body read", errHealthUnreachable)
	}
	if len(body) > healthcheckMaximumBody {
		return healthwire.Response{}, fmt.Errorf("%w: body too large", errHealthInvalid)
	}
	observation, err := healthwire.ParseExact(body)
	if err != nil {
		return healthwire.Response{}, fmt.Errorf("%w: body", errHealthInvalid)
	}
	if response.StatusCode == http.StatusOK && !observation.Ready ||
		response.StatusCode == http.StatusServiceUnavailable && observation.Ready {
		return healthwire.Response{}, fmt.Errorf("%w: status/body mismatch", errHealthInvalid)
	}
	return observation, nil
}
