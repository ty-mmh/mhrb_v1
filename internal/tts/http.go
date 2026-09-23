package tts

import (
	"context"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func validateMaximum(maximum int) error {
	if maximum < 1 || maximum > HardMaxAudioBytes {
		return &Error{Class: ErrorInvalidRequest}
	}
	return nil
}

func validateBaseURL(raw string, loopbackOnly bool) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", &Error{Class: ErrorInvalidRequest}
	}
	hostIP := net.ParseIP(parsed.Hostname())
	if loopbackOnly {
		if parsed.Scheme != "http" || hostIP == nil || !hostIP.IsLoopback() {
			return "", &Error{Class: ErrorInvalidRequest}
		}
	} else if parsed.Scheme != "https" && !(parsed.Scheme == "http" && hostIP != nil && hostIP.IsLoopback()) {
		return "", &Error{Class: ErrorInvalidRequest}
	}
	return strings.TrimRight(raw, "/"), nil
}

func noRedirectClient(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &copyClient
}

func responseMediaType(response *http.Response) (string, error) {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return "", &Error{Class: ErrorInvalidMedia}
	}
	return mediaType, nil
}

func readWAV(response *http.Response, maximum int) (Audio, error) {
	mediaType, err := responseMediaType(response)
	if err != nil || mediaType != MIMETypeWAV {
		return Audio{}, &Error{Class: ErrorInvalidMedia}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, int64(maximum)+1))
	if err != nil {
		return Audio{}, &Error{Class: ErrorTransport}
	}
	if len(data) > maximum {
		return Audio{}, &Error{Class: ErrorOutputTooLarge}
	}
	audio := Audio{MIMEType: MIMETypeWAV, Data: data}
	if err := audio.Validate(maximum); err != nil {
		return Audio{}, err
	}
	return audio, nil
}

func classifyHTTPError(response *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return &Error{Class: ErrorHTTP, StatusCode: response.StatusCode}
}

func classifyTransport(ctxErr, transportErr error) error {
	if errors.Is(ctxErr, context.DeadlineExceeded) || errors.Is(transportErr, context.DeadlineExceeded) {
		return &Error{Class: ErrorTimeout}
	}
	var timeout interface{ Timeout() bool }
	if errors.As(transportErr, &timeout) && timeout.Timeout() {
		return &Error{Class: ErrorTimeout}
	}
	return &Error{Class: ErrorTransport}
}
