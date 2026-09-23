package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"

	"mahoroba.local/mahoroba/internal/httpui"
)

// This probe needs no resident, provider, configuration file or database. The
// dialogue healthcheck remains a separate, strict readiness observation.
func runAdminHealthcheck(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin healthcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	target := flags.String("url", "http://127.0.0.1:8788/healthz", "loopback administration health URL")
	if duplicateHealthcheckFlag(arguments) {
		return errors.New("admin healthcheck: duplicate URL flag")
	}
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin healthcheck accepts flags only")
	}
	endpoint, address, err := normalizeHealthcheckURL(*target)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	client := newHealthcheckClient(address)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return errors.New("admin healthcheck: endpoint unreachable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
		return errors.New("admin healthcheck: invalid response")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(len(httpui.AdminHealthBody)+1)))
	if err != nil || string(body) != httpui.AdminHealthBody {
		return errors.New("admin healthcheck: invalid response")
	}
	_, err = fmt.Fprintln(stdout, "administration healthy")
	return err
}
